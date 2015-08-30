import re
import sys
import json
import shutil
import os.path
import warnings
import argparse
import texttable
import subprocess
import collections

import html

try:
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
except ImportError:
    plt = None
    mcolors = None

from hw_info import get_hw_info, b2ssize, ssize2b


default_report_templ = """
<!doctype html><html>

<head>
    <title>Ceph cluster report: {cluster_name} </title>
    <style type="text/css">
        {style}
        th {{
            text-align: center;
        }}
        td {{
            text-align: right;
        }}
    </style>
    {css_links}
    {script_links}
    {scripts}
</head>

<body onload="{onload}">
    {divs}
</body></html>
"""


class Report(object):
    def __init__(self, cluster_name, report_template=default_report_templ):
        self.cluster_name = cluster_name
        self.style = []
        self.style_links = []
        self.script_links = []
        self.scripts = []
        self.onload = []
        self.div_lines = []
        self.divs = []
        self.template = report_template

    def next_line(self):
        if len(self.divs) > 0:
            self.div_lines.append(self.divs)
            self.divs = []

    def __str__(self):
        style = "\n".join(self.style)
        css_links = "\n".join(
            '<link href="{0}" rel="stylesheet" type="text/css" />'.format(url)
            for url in self.style_links
        )

        script_links = "\n".join(
            '<script type="text/javascript" src="{0}"></script>'.format(url)
            for url in self.script_links
        )

        scripts = "\n".join(
            '<script type="text/javascript">{0}</script>'.format(script)
            for script in self.scripts)

        onload = ";".join(self.onload)
        # divs = "<br>\n".join(self.divs)

        self.next_line()

        divs = []
        for div_line in self.div_lines:
            divs.append('<center><table border="0" cellpadding="20"><tr><td>' +
                        "</td><td>".join(div_line) + "</td></tr></table></center><br>\n")

        return self.template.format(
            cluster_name=self.cluster_name,
            style=style,
            css_links=css_links,
            script_links=script_links,
            scripts=scripts,
            onload=onload,
            divs="".join(divs)
        )


class OSDStatus(object):
    pass


class RawResultStorage(object):
    def __init__(self, root):
        self._root = root
        self._all = None

    def _load(self):
        if self._all is None:
            self._all = {}
            for fname in os.listdir(self._root):
                if fname.startswith('.'):
                    continue

                full_path = os.path.abspath(os.path.join(self._root, fname))

                if os.path.isfile(full_path):
                    if '.' not in fname:
                        raise ValueError("File {0} has unknown type".format(full_path))
                    fname_no_ext, ext = fname.rsplit('.', 1)
                    self._all[fname_no_ext] = (True, ext, full_path)
                else:
                    self._all[fname] = (False, None, full_path)

        return self._all

    def __getattr__(self, name):
        self._load()
        path = os.path.join(self._root, name)
        if os.path.isdir(path):
            return True, None, self.__class__(path)

        if name in self._all:
            is_file, ext, full_path = self._all[name]
            setattr(self, name, (is_file, ext, full_path))

            if is_file:
                data = open(full_path, 'rb').read()
                return ext != 'err', ext, data
            else:
                return True, None, self.__class__(full_path)

        raise AttributeError(
            "No storage for {0!r} found. Have only '{1}' attrs".format(name, ",".join(self)))

    def __iter__(self):
        return iter(self._load().keys())

    def __getitem__(self, path):
        if '/' not in path:
            return getattr(self, path)

        item, rest = path.split('/', 1)
        is_ok, ext, data = getattr(self, item)
        if not is_ok:
            raise KeyError("error in path")
        if ext is not None:
            raise KeyError("Path not found")
        return data[rest]

    def __len__(self):
        return len(self._load())


class JResultStorage(object):
    def __init__(self, storage):
        self.__storage = storage
        self.__dct = []

    def __getattr__(self, name):
        is_ok, ext, data = getattr(self.__storage, name)

        if not is_ok:
            raise AttributeError("{0!r} contains error".format(name))
        elif ext is None:
            return self.__class__(data)
        elif ext != 'json':
            raise AttributeError("{0!r} have type {1!r}, not json".format(name, ext))

        res = json.loads(data)
        setattr(self, name, res)
        return res

    def __iter__(self):
        return iter(self.__storage)

    def __getitem__(self, path):
        if path not in self.__dct:
            is_ok, ext, data = self.__storage[path]

            if not is_ok:
                raise KeyError("{0!r} contains error".format(path))

            elif ext != 'json':
                raise KeyError("{0!r} have type {1!r}, not json".format(path, ext))

            self.__dct[path] = json.loads(data)
        return self.__dct[path]

    def __len__(self):
        return len(self.__storage)


def get_osds_info(jstorage):
    try:
        return jstorage._osd_info
    except AttributeError:
        pass

    osds_info = {}
    id2hosts = {}
    nodes = jstorage.master.osd_tree['nodes']

    for node in nodes:
        if node['type'] == "host":
            for child_id in node['children']:
                id2hosts[child_id] = node['name']

    for node in jstorage.master.osd_tree['nodes']:
        if node['type'] == "osd":
            stat = OSDStatus()
            stat.node = id2hosts[node['id']]
            stat.id = node['id']
            stat.status = node['status']
            stat.crush_weight = node['crush_weight']
            stat.reweight = node['reweight']
            osds_info[stat.id] = stat

    for osd_id in jstorage.osd:
        josd_obj = getattr(jstorage.osd, osd_id)

        osd_obj = osds_info[int(osd_id)]
        osd_obj.data_stor_stats = josd_obj.data.stats
        osd_obj.j_stor_stats = josd_obj.journal.stats
        osd_obj.__dict__.update(josd_obj.config)

    jstorage._osd_info = osds_info.values()
    return jstorage._osd_info


def calc_osd_pool_PG_distribution(jstorage):
    try:
        return jstorage._pg_distr
    except AttributeError:
        pass

    pool_id2name = dict((dt['poolnum'], dt['poolname'])
                        for dt in jstorage.master.osd_lspools)

    res = collections.defaultdict(lambda: collections.Counter())
    for pg in jstorage.master.pg_dump['pg_stats']:
        pool = int(pg['pgid'].split('.', 1)[0])
        for osd_num in pg['acting']:
            res[osd_num][pool_id2name[pool]] += 1

    all_pools = set()
    for item in res.values():
        all_pools.update(item.keys())
    pools = list(sorted(all_pools))

    sum_per_osd = dict(
        (name, sum(row.get(i, 0) for i in pools))
        for name, row in res.items()
    )

    sum_per_pool = [sum(osd_stat.get(pool_name, 0)
                    for osd_stat in res.values())
                    for pool_name in pools]

    jstorage._pg_distr = res, pools, sum_per_osd, sum_per_pool
    return jstorage._pg_distr


def show_osd_pool_PG_distribution_txt(jstorage):
    data, cols, sum_per_osd, sum_per_pool = calc_osd_pool_PG_distribution(jstorage)
    tab = texttable.Texttable(max_width=180)
    tab.set_deco(tab.HEADER | tab.VLINES | tab.BORDER)
    tab.set_cols_align(['l'] + ['r'] * (len(cols) + 1))
    tab.header(["OSD"] + map(str, cols) + ['sum'])

    for name, row in data.items():
        idata = [row.get(i, 0) for i in cols]
        tab.add_row([str(name)] +
                    map(str, idata) +
                    [str(sum_per_osd[name])])

    tab.add_row(["sum"] +
                map(str, sum_per_pool) +
                [str(sum(sum_per_pool))])

    return tab.draw()


def show_summary(report, jstorage):
    mstorage = jstorage.master
    line = '<tr><td>{0}:</td><td>{1}</td></tr>'
    res = [line.format("Status", mstorage.status['health']['overall_status'])]
    res.append(line.format("PG count", mstorage.status['pgmap']['num_pgs']))
    res.append(line.format("Pool count", len(mstorage.osd_lspools)))
    res.append(line.format("Used GB", mstorage.status['pgmap']["bytes_used"] / 1024 ** 3))
    res.append(line.format("Avail GB", mstorage.status['pgmap']["bytes_avail"] / 1024 ** 3))
    res.append(line.format("Data GB", mstorage.status['pgmap']["data_bytes"] / 1024 ** 3))

    avail_perc = mstorage.status['pgmap']["bytes_avail"] * 100 / \
        mstorage.status['pgmap']['bytes_total']
    res.append(line.format("Free %", avail_perc))

    osd_count = len(jstorage.osd)
    res.append(line.format("Mon count", len(mstorage.mon_status['monmap']['mons'])))

    report.divs.append(
        '<center>Status:<br><table border="0" cellpadding="5">' +
        "\n".join(res) +
        "</table></center>")

    res = []
    osd0_stats = get_osds_info(jstorage)[0]
    res.append(line.format("Count", osd_count))
    res.append(line.format("PG per OSD", mstorage.status['pgmap']['num_pgs'] / osd_count))
    res.append(line.format("Cluster net", osd0_stats.cluster_network))
    res.append(line.format("Public net", osd0_stats.public_network))
    res.append(line.format("Near full ratio", osd0_stats.mon_osd_nearfull_ratio))
    res.append(line.format("Full ratio", osd0_stats.mon_osd_full_ratio))
    res.append(line.format("Backfill full ratio", osd0_stats.osd_backfill_full_ratio))
    res.append(line.format("Filesafe full ratio", osd0_stats.osd_failsafe_full_ratio))
    res.append(line.format("Journal aio", osd0_stats.journal_aio))
    res.append(line.format("Journal dio", osd0_stats.journal_dio))

    report.divs.append(
        '<center>OSD:<table border="0" cellpadding="5">' +
        "\n".join(res) +
        "</table></center>")


html_ok = '<font color="green">{0}</font>'.format
html_fail = '<font color="red">{0}</font>'.format


def show_osd_info(report, jstorage, storage):
    data, cols, sum_per_osd, sum_per_pool = calc_osd_pool_PG_distribution(jstorage)
    table = html.Table(header_row=["OSD",
                                   "node",
                                   "status",
                                   "daemon<br>run",
                                   "weight<br>reweight",
                                   "PG count",
                                   "used GB",
                                   "free GB",
                                   "free %",
                                   "Journal<br>on same<br>disk",
                                   "Journal<br>on SSD",
                                   "Journal<br>on file"])

    for osd_stats in sorted(get_osds_info(jstorage), key=lambda x: x.id):
        used_b = osd_stats.data_stor_stats['used']
        avail_b = osd_stats.data_stor_stats['avail']

        avail_perc = int((avail_b * 100.0) / (avail_b + used_b) + 0.499999999999)

        if avail_perc < 20:
            color = "red"
        elif avail_perc < 40:
            color = "yellow"
        else:
            color = "green"

        try:
            ok, frmt, data = storage['osd/{0}/osd_daemons'.format(osd_stats.id)]
            for line in data.split("\n"):
                if 'ceph-osd' in line and '-i {0}'.format(osd_stats.id) in line:
                    daemon_msg = '<font color="green">yes</font>'
                    break
            else:
                daemon_msg = '<font color="red">no</font>'
        except KeyError:
            daemon_msg = '<font color="orange">???</font>'

        if osd_stats.data_stor_stats['root_dev'] == osd_stats.j_stor_stats['root_dev']:
            j_on_same_drive = html_fail("yes")
        else:
            j_on_same_drive = html_ok("no")

        if osd_stats.data_stor_stats['dev'] != osd_stats.j_stor_stats['dev']:
            j_on_file = html_ok("no")
        else:
            j_on_file = html_fail("yes")

        if osd_stats.j_stor_stats['is_ssd']:
            j_on_ssd = html_ok("yes")
        else:
            j_on_ssd = html_fail("no")

        if osd_stats.status == 'up':
            status = html_ok("up")
        else:
            status = html_fail("down")

        table.rows.append(
            map(str,
                [osd_stats.id,
                 osd_stats.node,
                 status,
                 daemon_msg,
                 "%.3f<br>%.3f" % (
                    float(osd_stats.crush_weight),
                    float(osd_stats.reweight)),
                 sum_per_osd[osd_stats.id],
                 used_b / 1024 ** 3,
                 avail_b / 1024 ** 3,
                 '<font color="{0}">{1}</font>'.format(color, avail_perc),
                 j_on_same_drive,
                 j_on_ssd,
                 j_on_file]))

    report.divs.append("<center><H3>OSD's info:</H3><br>\n" + str(table) + "</center>")


def show_mons_info(report, jstorage):
    table = html.Table(header_row=["Name",
                                   "Node",
                                   "Role",
                                   "Disk free<br>MB (%)"])
    srv_health = jstorage.master.status['health']['health']['health_services']
    assert len(srv_health) == 1
    for srv in srv_health[0]['mons']:

        if srv["health"] == "HEALTH_OK":
            health = html_ok("HEALTH_OK")
        else:
            health = html_fail(srv["health"])

        line = [
            srv["name"],
            health,
            "-",
            "{0} ({1})".format(srv["kb_avail"] / 1024, srv["avail_percent"])
        ]
        table.rows.append(map(str, line))

    report.divs.append("<center><H3>Monitors info:</H3><br>\n" + str(table) + "</center>")


def show_pools_info(report, jstorage):
    table = html.Table(header_row=["Pool",
                                   "size",
                                   "min_size",
                                   "Kobj",
                                   "data<br>MB",
                                   "free<br>MB",
                                   "read<br>MB",
                                   "write<br>MB",
                                   "ruleset",
                                   "PG"])

    _, pools, _, sum_per_pool = calc_osd_pool_PG_distribution(jstorage)

    pool_stats = {}
    for pool in jstorage.master.rados_df['pools']:
        assert len(pool['categories']) == 1
        pool_stats[pool['name']] = pool['categories'][0]

    for pool_name, data in sorted(jstorage.master.pool_stats.items()):
        stat = pool_stats[pool_name]
        vals = [
            pool_name,
            str(data['size']),
            str(data['min_size']),
            int(stat["num_objects"]) / 1024,
            int(stat["size_bytes"]) / 1024 ** 2,
            '---',
            int(stat["read_bytes"]) / 1024 ** 2,
            int(stat["write_bytes"]) / 1024 ** 2,
            str(data['crush_ruleset']),
            str(sum_per_pool[pools.index(pool_name)])]
        table.rows.append(map(str, vals))

    report.divs.append("<center><H3>Pool's stats:</H3><br>\n" + str(table) + "</center>")


def show_pg_state(report, jstorage):
    statuses = collections.defaultdict(lambda: 0)
    pgmap_stat = jstorage.master.status['pgmap']

    for pg_group in pgmap_stat['pgs_by_state']:
        for state_name in pg_group['state_name'].split('+'):
            statuses[state_name] += pg_group["count"]

    npg = pgmap_stat['num_pgs']
    table = html.Table(header_row=["Status", "Count", "%"])
    table.rows.append(["any", str(npg), "100.00"])
    for status, count in sorted(statuses.items()):
        table.rows.append([status, str(count), "%.2f" % (100.0 * count / npg)])

    report.divs.append("<center><H3>PG's status:</H3><br>\n" + str(table) + "</center>")


def show_osd_state(report, jstorage):
    statuses = collections.defaultdict(lambda: [])

    for osd_stat in get_osds_info(jstorage):
        statuses[osd_stat.status].append(
            "{0.node}:{0.id}".format(osd_stat))

    table = html.Table(header_row=["Status", "Count", "ID's"])
    for status, nodes in sorted(statuses.items()):
        table.rows.append([status, len(nodes),
                           "" if status == "up" else ",".join(nodes)])
    report.divs.append("<center><H3>OSD's state:</H3><br>\n" + str(table) + "</center>")


def get_node_mem_info(node):
    ok, frmt, meminfo = node.meminfo
    if not ok:
        return None

    info = {}
    assert frmt == 'txt'
    for line in meminfo.split("\n"):
        line = line.strip()
        if line == '':
            continue
        name, data = line.split(":", 1)
        data = data.strip()
        if " " in data:
            data = data.replace(" ", "")
            assert data[-1] == 'B'
            val = ssize2b(data[:-1])
        else:
            val = int(data)
        info[name] = val
    return info


def get_node_ip_addressed(node):
    ok, frmt, ipa = node.ipa
    if not ok:
        return None

    info = collections.defaultdict(lambda: [])
    assert frmt == 'txt'

    ip_rr_s = r"inet\s+(?P<ip>\d+\.\d+\.\d+\.\d+)/(?P<size>\d+)\s+.*?\s+scope\s+global\s+(?P<adapter>[^ \t\r\n]*)"

    for match in re.finditer(ip_rr_s, ipa):
        info[match.group('adapter')].append(match.group('ip'))

    return info


def get_node_load_5m(node):
    ok, frmt, loadavg = node.loadavg
    if not ok:
        return None
    return int(float(loadavg.strip().split()[1]))


def show_hosts_stats(report, storage):
    header_row = ["Hostname", "CPU's", "RAM<br>total", "RAM<br>free",
                  "Swap<br>used", "Net info<br>Dev, speed, duplex<br>[IP addrs]", "Load avg<br>5 min"]
    table = html.Table(header_row=header_row)
    ok, _, hosts = storage.hosts
    assert ok

    for host_name in sorted(hosts):
        node = getattr(hosts, host_name)[2]
        ok, frmt, lshw = node.lshw
        if not ok:
            table.rows.append([host_name] + ['-'] * (header_row - 1))
            continue
        assert frmt == 'xml'

        hw_info = get_hw_info(lshw)

        host_info = []
        if hw_info.cores == []:
            host_info.append("Error")
        else:
            host_info.append(sum(count for _, count in hw_info.cores))

        mem_info = get_node_mem_info(node)
        host_info.append(b2ssize(mem_info['MemTotal']))
        host_info.append(b2ssize(mem_info['MemFree']))
        host_info.append(b2ssize(mem_info['SwapTotal'] - mem_info['SwapFree']))

        ip_addr = get_node_ip_addressed(node)

        if hw_info.net_info != {}:
            net_info = []
            for name, (speed, dtype, _) in hw_info.net_info.items():
                net_info.append("{0}, {1}, {2}".format(name, speed, dtype))
                if name in ip_addr:
                    net_info[-1] += "<br>" + ",".join(ip_addr[name])
            host_info.append("<br/>".join(net_info))
        else:
            host_info.append("Error")

        host_info.append(get_node_load_5m(node))

        table.rows.append([host_name] + map(str, host_info))

    report.divs.append("<center><H3>Host's info:</H3><br>\n" + str(table) + "</center>")


def show_osd_pool_PG_distribution_html(report, jstorage):
    data, cols, sum_per_osd, sum_per_pool = calc_osd_pool_PG_distribution(jstorage)
    table = html.Table(header_row=["OSD/pool"] + map(str, cols) + ['sum'])

    for name, row in sorted(data.items()):
        idata = [row.get(i, 0) for i in cols]
        table.rows.append([str(name)] +
                          map(str, idata) +
                          [str(sum_per_osd[name])])

    table.rows.append(["sum"] +
                      map(str, sum_per_pool) +
                      [str(sum(sum_per_pool))])

    report.divs.append("<center><H3>PG per OSD:</H3><br>" + str(table) + "</center>")


visjs_script = """
  var network__id__ = null;
  function draw__id__() {
    if (network__id__ !== null) network__id__.destroy();
    var data = {nodes: [__nodes__], edges: [__eges__]};
    var options = {
      // layout: { hierarchical: {sortMethod: 'directed'}},
        edges: {smooth: true, arrows: {to : true }},
        nodes: {shape: 'dot'}
    };
    network__id__ = new vis.Network(document.getElementById('mynetwork__id__'), data, options);
  }
"""

visjs_css = """
body {font: 10pt sans;}
.graph {width: 500px;height: 500px;border: 1px solid lightgray;}
"""

# mynetwork {width: 500px;height: 500px;border: 1px solid lightgray;}
# mynetwork {width: 500px;height: 500px;border: 1px solid lightgray;}
# mynetwork {width: 500px;height: 500px;border: 1px solid lightgray;}


def tree_to_visjs(report, jstorage):
    report.style.append(visjs_css)
    report.style_links.append(
        "https://cdnjs.cloudflare.com/ajax/libs/vis/4.7.0/vis.min.css")
    report.script_links.append(
        "https://cdnjs.cloudflare.com/ajax/libs/vis/4.7.0/vis.min.js"
    )

    nodes = jstorage.master.osd_tree["nodes"]

    max_w = max(float(node['crush_weight'])
                for node in nodes
                if node['type'] == 'osd')

    min_w = min(float(node['crush_weight'])
                for node in nodes
                if node['type'] == 'osd')

    cmap = plt.get_cmap('rainbow')

    def get_color_w(node):
        if max_w - min_w < 1E-2 or node['type'] != 'osd':
            return "#ffffff"
        w = (float(node['crush_weight']) - min_w) / (max_w - min_w)
        return str(mcolors.rgb2hex(cmap(w)))

    _, _, sum_per_osd, _ = calc_osd_pool_PG_distribution(jstorage)

    min_pg = min(sum_per_osd.values())
    max_pg = max(sum_per_osd.values())

    def get_color_pg_count(node):
        if (max_w - min_w) / float(max_w) < 1E-2 or node['type'] != 'osd':
            return "#ffffff"

        w = (float(sum_per_osd[node['id']]) - min_pg) / (max_pg - min_pg)
        return str(mcolors.rgb2hex(cmap(w)))

    def get_graph(color_func):
        nodes_list = []
        eges_list = []
        if plt is not None:
            nodes_list = [
                "{{id:{0}, label:'{1}', color:'{2}'}}".format(
                    node['id'],
                    str(node['name']),
                    color_func(node)
                )
                for node in nodes
            ]
        else:
            nodes_list = [
                "{{id:{0}, label:'{1}'}}".format(node['id'], str(node['name']))
                for node in nodes
            ]

        for node in nodes:
            for child_id in node.get('children', []):
                eges_list.append(
                    "{{from: {0}, to: {1} }}".format(node['id'], child_id)
                )

        return ",".join(nodes_list), ",".join(eges_list)

    report.next_line()
    gnodes, geges = get_graph(get_color_w)
    report.scripts.append(
        visjs_script.replace('__nodes__', gnodes)
                    .replace('__eges__', geges)
                    .replace('__id__', '0')
    )
    report.divs.append('<center>Crush weight:</center><br><div class="graph" id="mynetwork0"></div>')
    report.onload.append("draw0()")

    gnodes, geges = get_graph(get_color_pg_count)
    report.scripts.append(
        visjs_script.replace('__nodes__', gnodes)
                    .replace('__eges__', geges)
                    .replace('__id__', '1')
    )
    report.divs.append('<center>PG\'s count:</center><br><div class="graph" id="mynetwork1"></div>')
    report.onload.append("draw1()")
    report.next_line()


node_templ_gv = '{0} [label="{1}"];'
link_templ_gv = '{0} -> {1};'


def getid(oid):
    return ("N" + str(oid) if oid >= 0 else "NN" + str(-oid))


def tree_to_graphviz(jstorage):
    res = "digraph cluster{"
    nodes = jstorage.master.osd_tree["nodes"]

    for node in nodes:
        res += node_templ_gv.format(getid(node['id']), str(node['name']))

    for node in nodes:
        for child_id in node.get('children', []):
            res += link_templ_gv.format(getid(node['id']), getid(child_id))

    return res + "}"


node_templ = ".addNode({{id: '{0}', label: '{1}', size: 1}})"
link_templ = ".addEdge({{id: 'e{0}', source: '{1}', target: '{2}'}})"


def tree_to_sigma(jstorage):
    res = "s.graph"
    nodes = jstorage.master.osd_tree["nodes"]

    for node in nodes:
        res += node_templ.format(getid(node['id']), node['name'])

    uniq_id = 0
    for node in nodes:
        for uniq_id, child_id in enumerate(node.get('children', []), uniq_id):
            res += link_templ.format(uniq_id, getid(node['id']), getid(child_id))

    return res


def parse_args(argv):
    p = argparse.ArgumentParser()
    p.add_argument("report_name", help="Report name")
    p.add_argument("data_folder", help="Folder with data, or .tar.gz archive")
    return p.parse_args(argv[1:])


def main(argv):
    opts = parse_args(argv)
    remove_folder = False

    if os.path.isfile(opts.data_folder):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            arch_name = opts.data_folder
            folder = os.tempnam()
            os.makedirs(folder)
            remove_folder = True
            subprocess.call("tar -zxvf {0} -C {1} >/dev/null 2>&1".format(arch_name, folder), shell=True)
    else:
        folder = opts.data_folder

    if not os.path.isdir(folder):
        print "First argument should be a folder with data or path to archive"
        return 1

    try:
        storage = RawResultStorage(folder)
        jstorage = JResultStorage(storage)
        report = Report(opts.report_name)
        show_summary(report, jstorage)
        report.next_line()

        show_osd_pool_PG_distribution_html(report, jstorage)
        report.next_line()

        show_pools_info(report, jstorage)
        show_osd_info(report, jstorage, storage)
        show_pg_state(report, jstorage)
        report.next_line()

        show_osd_state(report, jstorage)
        show_hosts_stats(report, storage)
        show_mons_info(report, jstorage)
        report.next_line()

        tree_to_visjs(report, jstorage)
        print str(report)
    finally:
        if remove_folder:
            shutil.rmtree(folder)


if __name__ == "__main__":
    exit(main(sys.argv))
