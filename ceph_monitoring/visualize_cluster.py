import re
import sys
import json
import shutil
import bisect
import os.path
import warnings
import argparse
import subprocess
import collections

from ipaddr import IPNetwork, IPAddress

import html

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

    def get(self, path, default=None, expected_format='txt'):
        ok, frmt, data = self[path]
        if not ok or frmt != expected_format:
            return default
        return data

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

        try:
            osd_obj.__dict__.update(josd_obj.config)
        except AttributeError:
            # osd is down, no config available
            pass

    jstorage._osd_info = list(osds_info.values())
    return jstorage._osd_info


def osd_pool_PG_distribution(jstorage):
    try:
        return jstorage._pg_distr
    except AttributeError:
        pass

    try:
        pg_dump = jstorage.master.pg_dump
    except AttributeError:
        return None, None, None

    pool_id2name = dict((dt['poolnum'], dt['poolname'])
                        for dt in jstorage.master.osd_lspools)

    osd_pool_pg_2d = collections.defaultdict(lambda: collections.Counter())
    sum_per_pool = collections.Counter()
    sum_per_osd = collections.Counter()

    for pg in pg_dump['pg_stats']:
        pool = int(pg['pgid'].split('.', 1)[0])
        for osd_num in pg['acting']:
            pool_name = pool_id2name[pool]
            osd_pool_pg_2d[osd_num][pool_name] += 1
            sum_per_pool[pool_name] += 1
            sum_per_osd[osd_num] += 1

    jstorage._pg_distr = osd_pool_pg_2d, sum_per_osd, sum_per_pool
    return jstorage._pg_distr


def get_alive_osd_stats(jstorage):
    # try to find alive osd
    for osd_stats in get_osds_info(jstorage):
        if hasattr(osd_stats, 'mon_osd_full_ratio'):
            return osd_stats
    return None


def get_ceph_nets(osd_stats):
    if osd_stats is None:
        return None, None

    return getattr(osd_stats, 'cluster_network', None), \
        getattr(osd_stats, 'public_network', None)


def show_summary(report, jstorage, storage):
    mstorage = jstorage.master

    line = '<tr><td>{0}:</td><td>{1}</td></tr>'

    ok, frmt, data = storage['master/collected_at']
    assert ok and frmt == 'txt'

    res = []

    def ap(x, y):
        res.append(line.format(x, y))

    local, gmt, _ = data.strip().split("\n")

    ap("Collected at", local)
    ap("Collected at GMT", gmt)
    ap("Status", mstorage.status['health']['overall_status'])
    ap("PG count", mstorage.status['pgmap']['num_pgs'])
    ap("Pool count", len(mstorage.osd_lspools))
    ap("Used GB", mstorage.status['pgmap']["bytes_used"] / 1024 ** 3)
    ap("Avail GB", mstorage.status['pgmap']["bytes_avail"] / 1024 ** 3)
    ap("Data GB", mstorage.status['pgmap']["data_bytes"] / 1024 ** 3)

    avail_perc = mstorage.status['pgmap']["bytes_avail"] * 100 / \
        mstorage.status['pgmap']['bytes_total']
    ap("Free %", avail_perc)

    osd_count = len(jstorage.osd)
    ap("Mon count", len(mstorage.mon_status['monmap']['mons']))

    report.divs.append(
        '<center>Status:<br><table border="0" cellpadding="5">' +
        "\n".join(res) +
        "</table></center>")

    del res[:]

    osd_stats = get_alive_osd_stats(jstorage)

    if osd_stats is None:
        res.append('<font color="red"><H3>No live OSD found!</H3></font><br>')
    else:
        pub_net, cluster_net = get_ceph_nets(osd_stats)

        ap("Count", osd_count)
        ap("PG per OSD", mstorage.status['pgmap']['num_pgs'] / osd_count)
        ap("Cluster net", cluster_net)
        ap("Public net", pub_net)
        ap("Near full ratio", osd_stats.mon_osd_nearfull_ratio)
        ap("Full ratio", osd_stats.mon_osd_full_ratio)
        ap("Backfill full ratio", osd_stats.osd_backfill_full_ratio)
        ap("Filesafe full ratio", osd_stats.osd_failsafe_full_ratio)
        ap("Journal aio", osd_stats.journal_aio)
        ap("Journal dio", osd_stats.journal_dio)

    report.divs.append(
        '<center>OSD:<table border="0" cellpadding="5">' +
        "\n".join(res) +
        "</table></center>")
    del res[:]

    ap("Client IO MBps",
        "%0.2f" % (mstorage.status['pgmap'].get('write_bytes_sec', 0) / 2.0 ** 20))
    ap("Client IO IOPS", mstorage.status['pgmap'].get('op_per_sec', 0))

    report.divs.append(
        '<center>Activity:<table border="0" cellpadding="5">' +
        "\n".join(res) +
        "</table></center>")
    del res[:]

    report.next_line()

    messages = ""
    for msg in mstorage.status['health']['summary']:
        if msg['severity'] == "HEALTH_WARN":
            color = "orange"
        elif msg['severity'] == "HEALTH_ERR":
            color = "red"
        else:
            color = "black"

        messages += '<font color="{0}">{1}</font><br>\n'.format(color, msg['summary'])

    report.divs.append(messages)


def show_mons_info(report, jstorage, storage):
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


def show_pg_state(report, jstorage, storage):
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


def show_osd_state(report, cluster):
    statuses = collections.defaultdict(lambda: [])

    for osd in cluster.osds:
        statuses[osd.status].append("{0.host}:{0.id}".format(osd))

    table = html.Table(header_row=["Status", "Count", "ID's"])
    for status, osds in sorted(statuses.items()):
        table.rows.append([status, len(osds),
                           "" if status == "up" else ",".join(osds)])
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


def get_node_net_stats(host_name, storage):
    ok, frmt, netdev = storage['hosts/%s/netdev' % host_name]
    if not ok:
        return None
    assert frmt == 'txt'

    cols_s = "rbytes rpackets rerrs rdrop rfifo rframe rcompressed "
    cols_s += "rmulticast sbytes spackets serrs sdrop sfifo scolls "
    cols_s += "scarrier scompressed"
    cols = cols_s.split()

    info = {}
    for line in netdev.strip().split("\n")[2:]:
        adapter, data = line.split(":")
        assert adapter not in info
        info[adapter] = dict(zip(cols, map(int, data.split())))

    return info


class Maybe(object):
    def __init__(self, tp, obj=None):
        self.__tp = tp
        self.__obj = obj


class CephOSD(object):
    def __init__(self):
        self.id = None
        self.status = None
        self.host = None
        self.daemon_runs = None
        self.pg_count = None


def find(lst, check, default=None):
    for obj in lst:
        if check(obj):
            return obj
    return default


class CephCluster(object):
    def __init__(self, jstorage, storage):
        self.osds = []
        self.mons = []
        self.pools = {}
        self.nodes = []

        self.osd_tree = {}
        self.osd_tree_root_id = None
        # self.osd_tree_hosts_ids = []
        # self.osd_tree_osd_ids = []

        self.cluster_net = Maybe(IPNetwork)
        self.public_net = Maybe(IPNetwork)
        self.storage = storage
        self.jstorage = jstorage

    def load(self):
        self.load_osd_tree()
        self.osd_pool_pg_2d, \
            self.sum_per_osd, \
            self.sum_per_pool = osd_pool_PG_distribution(self.jstorage)

        self.load_cluster_networks()
        self.load_osds()
        self.load_pools()

    def load_cluster_networks(self):
        osd_stats = get_alive_osd_stats(self.jstorage)

        if osd_stats is not None:
            pub_net_str, cluster_net_str = get_ceph_nets(osd_stats)
            self.public_net = IPNetwork(pub_net_str)
            self.cluster_net = IPNetwork(cluster_net_str)
        else:
            self.cluster_net = None
            self.public_net = None

    def load_osd_tree(self):
        nodes = self.jstorage.master.osd_tree['nodes']

        self.osd_tree_root_id = nodes[0]['id']
        self.osd_tree = dict((node['id'], node) for node in nodes)

        # set backtrack links
        def fill_parent(obj, parent_id=None):
            obj['parent'] = parent_id
            if 'children' in obj:
                for child_id in obj['children']:
                    fill_parent(self.osd_tree[child_id], obj['id'])

        fill_parent(self.osd_tree[self.osd_tree_root_id])

    def find_host_for_node(self, node):
        cnode = node
        while cnode['type'] != 'host':
            if cnode['parent'] is None:
                raise IndexError("Can't found host for " + str(node['id']))
            cnode = self.osd_tree[cnode['parent']]
        return cnode

    def load_osds(self):
        self.osds = [load_osd(self.jstorage, self.storage,
                              node, self.find_host_for_node(node))
                     for node in self.osd_tree.values()
                     if node['type'] == 'osd']
        self.osds.sort(key=lambda x: x.id)

    def load_pools(self):
        self.pools = {}

        for pool_part in self.jstorage.master.osd_dump['pools']:
            pool = Pool()
            pool.id = pool_part['pool']
            pool.name = pool_part['pool_name']
            pool.__dict__.update(pool_part)
            self.pools[int(pool.id)] = pool

        for pool_part in self.jstorage.master.rados_df['pools']:
            assert len(pool_part['categories']) == 1
            cat = pool_part['categories'][0].copy()
            del cat['name']
            self.pools[int(pool_part['id'])].__dict__.update(cat)


class Pool(object):
    def __init__(self):
        self.id = None
        self.name = None


def show_pools_info(report, cluster):
    table = html.Table(header_row=["Pool",
                                   "Id",
                                   "size",
                                   "min_size",
                                   "Kobj",
                                   "data<br>MB",
                                   "free<br>MB",
                                   "read<br>MB",
                                   "write<br>MB",
                                   "ruleset",
                                   "PG",
                                   "PGP"])

    for _, pool in sorted(cluster.pools.items()):
        vals = [pool.name,
                pool.id,
                pool.size,
                pool.min_size,
                int(pool.num_objects) / 1024,
                int(pool.size_bytes) / 1024 ** 2,
                '---',
                int(pool.read_bytes) / 1024 ** 2,
                int(pool.write_bytes) / 1024 ** 2,
                pool.crush_ruleset,
                pool.pg_num,
                pool.pg_placement_num]
        table.rows.append(map(str, vals))

    report.divs.append("<center><H3>Pool's stats:</H3><br>\n" + str(table) + "</center>")


def load_osd(jstorage, storage, node, host):
    osd = CephOSD()
    osd.__dict__.update(node)
    osd.host = host['name']

    try:
        osd_data = getattr(jstorage.osd, str(node['id']))
        osd.data_stor_stats = osd_data.data.stats
        osd.j_stor_stats = osd_data.journal.stats
    except AttributeError:
        osd.data_stor_stats = None
        osd.j_stor_stats = None

    osd.osd_perf = find(jstorage.master.osd_perf["osd_perf_infos"],
                        lambda x: x['id'] == osd.id)["perf_stats"]

    data = storage.get('osd/{0}/osd_daemons'.format(osd.id))
    if data is None:
        osd.daemon_runs = None
    else:
        for line in data.split("\n"):
            if 'ceph-osd' in line and '-i {0}'.format(osd.id) in line:
                osd.daemon_runs = True
                break
        else:
            osd.daemon_runs = False

    _, sum_per_osd, _ = osd_pool_PG_distribution(jstorage)

    if sum_per_osd is not None:
        osd.pg_count = sum_per_osd[osd.id]
    else:
        osd.pg_count = None

    return osd


HTML_UNKNOWN = '<font color="orange">???</font>'
html_ok = '<font color="green">{0}</font>'.format
html_fail = '<font color="red">{0}</font>'.format


def show_osd_info(report, cluster):
    table = html.Table(header_row=["OSD",
                                   "node",
                                   "status",
                                   "daemon<br>run",
                                   "weight<br>reweight",
                                   "PG count",
                                   "used GB",
                                   "free GB",
                                   "free %",
                                   "apply lat<br>ms",
                                   "commit lat<br>ms",
                                   "Journal<br>on same<br>disk",
                                   "Journal<br>on SSD",
                                   "Journal<br>on file"])

    for osd in cluster.osds:
        if osd.daemon_runs is None:
            daemon_msg = HTML_UNKNOWN
        elif osd.daemon_runs:
            daemon_msg = '<font color="green">yes</font>'
        else:
            daemon_msg = '<font color="red">no</font>'

        if osd.data_stor_stats is not None:
            used_b = osd.data_stor_stats.get('used')
            avail_b = osd.data_stor_stats.get('avail')
            avail_perc = int((avail_b * 100.0) / (avail_b + used_b) + 0.5)

            used_gb = used_b / 1024 ** 3
            avail_gb = avail_b / 1024 ** 3

            if avail_perc < 20:
                color = "red"
            elif avail_perc < 40:
                color = "yellow"
            else:
                color = "green"
            avail_perc_str = '<font color="{0}">{1}</font>'.format(color, avail_perc)

            if osd.data_stor_stats['root_dev'] == osd.j_stor_stats['root_dev']:
                j_on_same_drive = html_fail("yes")
            else:
                j_on_same_drive = html_ok("no")

            if osd.data_stor_stats['dev'] != osd.j_stor_stats['dev']:
                j_on_file = html_ok("no")
            else:
                j_on_file = html_fail("yes")

            if osd.j_stor_stats['is_ssd']:
                j_on_ssd = html_ok("yes")
            else:
                j_on_ssd = html_fail("no")
        else:
            used_gb = HTML_UNKNOWN
            avail_gb = HTML_UNKNOWN
            avail_perc_str = HTML_UNKNOWN
            j_on_same_drive = HTML_UNKNOWN
            j_on_file = HTML_UNKNOWN

        if osd.status == 'up':
            status = html_ok("up")
        else:
            status = html_fail("down")

        if osd.osd_perf is not None:
            apply_latency_ms = osd.osd_perf["apply_latency_ms"]
            commit_latency_ms = osd.osd_perf["commit_latency_ms"]
        else:
            apply_latency_ms = HTML_UNKNOWN
            commit_latency_ms = HTML_UNKNOWN

        if osd.pg_count is None:
            pg_count = HTML_UNKNOWN
        else:
            pg_count = osd.pg_count

        table.rows.append(
            map(str,
                [osd.id,
                 osd.host,
                 status,
                 daemon_msg,
                 "%.3f<br>%.3f" % (
                    float(osd.crush_weight),
                    float(osd.reweight)),
                 pg_count,
                 used_gb,
                 avail_gb,
                 avail_perc_str,
                 apply_latency_ms,
                 commit_latency_ms,
                 j_on_same_drive,
                 j_on_ssd,
                 j_on_file]))

    report.divs.append("<center><H3>OSD's info:</H3><br>\n" + str(table) + "</center>")


class Node(object):
    def __init__(self, name):
        self.name = name
        self.ceph_cluster_adapter = Maybe(NodeNetworkInfo)
        self.ceph_public_adapter = Maybe(NodeNetworkInfo)
        self.net_adapters = {}
        self.disks = {}


class NodeNetworkInfo(object):
    def __init__(self):
        self.adapter = None
        self.network = None
        self.ip = None
        self.perf_stats = None


def get_node_ceph_net_stats(host_name, jstorage, storage):
    ok, frmt, ipa = storage['hosts/%s/ipa' % host_name]
    assert ok and frmt == 'txt'

    ip_rr_s = r"\d+:\s+(?P<adapter>.*?)\s+inet\s+(?P<ip>\d+\.\d+\.\d+\.\d+)/(?P<size>\d+)"
    info = collections.defaultdict(lambda: [])
    for line in ipa.split("\n"):
        match = re.match(ip_rr_s, line)
        if match is not None:
            info[match.group('adapter')].append(
                (match.group('ip'), match.group('size')))

    osd_stats = get_alive_osd_stats(jstorage)

    # node = CephNode()
    node = type('CephNode', (object,), {})()

    if osd_stats is not None:
        pub_net_str, cluster_net_str = get_ceph_nets(osd_stats)
    else:
        return node

    node.cluster_net = NodeNetworkInfo()
    node.public_net = NodeNetworkInfo()

    node.public_net.network = IPNetwork(pub_net_str)
    node.cluster_net.network = IPNetwork(cluster_net_str)

    for adapter, ips_with_sizes in info.items():
        for ip, sz in ips_with_sizes:
            if IPAddress(ip) in node.public_net.network:
                node.public_net.adapter = adapter
                node.public_net.ip = ip

            if IPAddress(ip) in node.cluster_net.network:
                node.cluster_net.adapter = adapter
                node.cluster_net.ip = ip

    net_stats = get_node_net_stats(host_name, storage)
    for net in (node.cluster_net, node.public_net):
        if net.adapter is not None:
            net.perf_stats = net_stats.get(net.adapter)

    return node


def get_node_load_5m(node):
    ok, frmt, loadavg = node.loadavg
    if not ok:
        return None
    return int(float(loadavg.strip().split()[1]))


def show_hosts_stats(report, jstorage, storage):
    header_row = ["Hostname",
                  "Ceph services",
                  "CPU's",
                  "RAM<br>total",
                  "RAM<br>free",
                  "Swap<br>used",
                  "Cluster net<br>dev, ip<br>settings",
                  "Cluster net<br>average<br>send/recv",
                  "Cluster net<br>current<br>send/recv",
                  "Public net<br>dev, ip<br>settings",
                  "Public net<br>average<br>send/recv",
                  "Public net<br>current<br>send/recv",
                  "Load avg<br>5 min"]
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

        services = ["osd-{0}".format(osd_stat.id)
                    for osd_stat in get_osds_info(jstorage)
                    if osd_stat.node == host_name]

        all_mons = [mon_data['name'] for mon_data in jstorage.master.mon_status['monmap']['mons']]
        if host_name in all_mons:
            services.append("mon(" + host_name + ")")

        host_info = [host_name, "<br>".join(services)]

        hw_info = get_hw_info(lshw)

        if hw_info.cores == []:
            host_info.append("Error")
        else:
            host_info.append(sum(count for _, count in hw_info.cores))

        mem_info = get_node_mem_info(node)
        host_info.append(b2ssize(mem_info['MemTotal']))
        host_info.append(b2ssize(mem_info['MemFree']))
        host_info.append(b2ssize(mem_info['SwapTotal'] - mem_info['SwapFree']))

        ceph_node = get_node_ceph_net_stats(host_name, jstorage, storage)

        for net in (ceph_node.cluster_net, ceph_node.public_net):
            if net is None:
                host_info.append("No data")
                host_info.append("No data")
            else:
                dev_ip = "{0}<br>{1}".format(net.adapter, net.ip)
                if net.adapter not in hw_info.net_info:
                    settings = "No data"
                else:
                    speed, dtype, _ = hw_info.net_info[net.adapter]
                    settings = "{0}, {1}".format(speed, dtype)
                host_info.append("{0}<br>{1}".format(dev_ip, settings))

                host_info.append("{0} / {1}<br>{2} / {3}".format(
                    net.perf_stats['sbytes'] / 1024 ** 2,
                    net.perf_stats['rbytes'] / 1024 ** 2,
                    net.perf_stats['spackets'] / 1000,
                    net.perf_stats['rpackets'] / 1000
                ))
                host_info.append('---')

        host_info.append(get_node_load_5m(node))
        table.rows.append(map(str, host_info))

    report.divs.append("<center><H3>Host's info:</H3><br>\n" + str(table) + "</center>")


def show_osd_pool_PG_distribution_html(report, cluster):
    if cluster.sum_per_osd is None:
        report.divs.append("<center><H3>PG per OSD: No pg dump data. Probably too many PG</H3></center>")
        return

    pools = sorted(cluster.sum_per_pool)
    table = html.Table(header_row=["OSD/pool"] + list(pools) + ['sum'])

    for osd_id, row in sorted(cluster.osd_pool_pg_2d.items()):
        data = [osd_id] + \
               [row.get(pool_name, 0) for pool_name in pools] + \
               [cluster.sum_per_osd[osd_id]]

        table.rows.append(map(str, data))

    table.rows.append(["sum"] +
                      [cluster.sum_per_pool[pool_name] for pool_name in pools] +
                      [str(sum(cluster.sum_per_pool.values()))])

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

def_color_map = [
    (0.0, (0.500, 0.000, 1.000)),
    (0.1, (0.304, 0.303, 0.988)),
    (0.2, (0.100, 0.588, 0.951)),
    (0.3, (0.096, 0.805, 0.892)),
    (0.4, (0.300, 0.951, 0.809)),
    (0.5, (0.504, 1.000, 0.705)),
    (0.6, (0.700, 0.951, 0.588)),
    (0.7, (0.904, 0.805, 0.451)),
    (0.8, (1.000, 0.588, 0.309)),
    (0.9, (1.000, 0.303, 0.153)),
    (1.0, (1.000, 0.000, 0.000))
]


def val_to_color(val, color_map=def_color_map):
    idx = [i[0] for i in color_map]
    assert idx == sorted(idx)

    pos = bisect.bisect_left(idx, val)

    if pos <= 0:
        return color_map[0][1]

    if pos > len(idx):
        return color_map[-1][1]

    color1 = color_map[pos - 1][1]
    color2 = color_map[pos][1]

    dx1 = (val - idx[pos - 1]) / (idx[pos] - idx[pos - 1])
    dx2 = (idx[pos] - val) / (idx[pos] - idx[pos - 1])

    ncolor = [(v1 * dx2 + v2 * dx1) * 255 for v1, v2 in zip(color1, color2)]
    return "#%02X%02X%02X" % tuple(map(int, ncolor))


def tree_to_visjs(report, jstorage, storage):
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

    def get_color_w(node):
        if max_w - min_w < 1E-2 or node['type'] != 'osd':
            return "#ffffff"
        w = (float(node['crush_weight']) - min_w) / (max_w - min_w)
        return val_to_color(w)

    try:
        _, sum_per_osd, _ = osd_pool_PG_distribution(jstorage)
        min_pg = min(sum_per_osd.values())
        max_pg = max(sum_per_osd.values())
    except AttributeError:
        min_pg = max_pg = sum_per_osd = None

    def get_color_pg_count(node):
        if (max_pg - min_pg) / float(max_pg) < 1E-2 or node['type'] != 'osd':
            return "#ffffff"

        w = (float(sum_per_osd[node['id']]) - min_pg) / (max_pg - min_pg)
        return val_to_color(w)

    def get_graph(color_func):
        nodes_list = []
        eges_list = []
        nodes_list = [
            "{{id:{0}, label:'{1}', color:'{2}'}}".format(
                node['id'],
                str(node['name']),
                color_func(node)
            )
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

    if sum_per_osd is not None:
        gnodes, geges = get_graph(get_color_pg_count)
        report.scripts.append(
            visjs_script.replace('__nodes__', gnodes)
                        .replace('__eges__', geges)
                        .replace('__id__', '1')
        )
        report.divs.append('<center>PG\'s count:</center><br><div class="graph" id="mynetwork1"></div>')
        report.onload.append("draw1()")
    report.next_line()


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

        cluster = CephCluster(jstorage, storage)
        cluster.load()

        report = Report(opts.report_name)

        show_summary(report, jstorage, storage)
        report.next_line()

        show_osd_pool_PG_distribution_html(report, cluster)
        report.next_line()

        show_osd_info(report, cluster)
        report.next_line()

        show_pools_info(report, cluster)
        show_pg_state(report, jstorage, storage)
        report.next_line()

        show_osd_state(report, cluster)
        show_hosts_stats(report, jstorage, storage)
        show_mons_info(report, jstorage, storage)
        report.next_line()

        tree_to_visjs(report, jstorage, storage)
        print str(report)
    finally:
        if remove_folder:
            shutil.rmtree(folder)


if __name__ == "__main__":
    exit(main(sys.argv))
