import sys
import json
import shutil
import os.path
import warnings
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


# PG per OSD
# object per OSD
# MB per OSD

default_report_templ = """
<!doctype html><html>

<head>
    <title>Ceph cluster report: {cluster_name} </title>
    <style type="text/css">
        {style}
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
        self.divs = []
        self.template = report_template

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
        divs = "<br>\n".join(self.divs)

        return self.template.format(
            cluster_name=self.cluster_name,
            style=style,
            css_links=css_links,
            script_links=script_links,
            scripts=scripts,
            onload=onload,
            divs=divs
        )


class OSDStatus(object):
    pass


class ClusterData(object):
    def __init__(self, folder):
        self._folder = folder
        self._osd_info = None
        self._files = {}

    def __iter__(self):
        self._fill_osd_info()
        return iter(self._osd_info.values())

    def __getitem__(self, osd_num):
        self._fill_osd_info()
        return self._osd_info[osd_num]

    def __len__(self):
        self._fill_osd_info()
        return len(self._osd_info)

    def __getattr__(self, name):
        try:
            fname = os.path.join(self._folder, "master", name) + ".json"
            val = json.loads(open(fname).read())
        except Exception as exc:
            raise AttributeError(str(exc))
        setattr(self, name, val)
        return val

    def _fill_osd_info(self):
        if self._osd_info is not None:
            return

        self._osd_info = {}
        id2hosts = {}

        for node in self.osd_tree['nodes']:
            if node['type'] == "host":
                for child_id in node['children']:
                    id2hosts[child_id] = node['name']

        for node in self.osd_tree['nodes']:
            if node['type'] == "osd":
                stat = OSDStatus()
                stat.node = id2hosts[node['id']]
                stat.id = node['id']
                stat.status = node['status']
                self._osd_info[stat.id] = stat

        for osd_folder in os.listdir(self._folder):
            if osd_folder.startswith('osd-'):
                cfg_file = os.path.join(self._folder, osd_folder, "config.json")
                data = json.loads(open(cfg_file).read())
                osd_id = int(osd_folder.split('-')[1])
                self._osd_info[osd_id].__dict__.update(data)

        return


def calc_osd_pool_PG_distribution(cdata):
    pool_id2name = dict((dt['poolnum'], dt['poolname']) for dt in cdata.osd_lspools)

    res = collections.defaultdict(lambda: collections.Counter())
    for pg in cdata.pg_dump['pg_stats']:
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

    return res, pools, sum_per_osd, sum_per_pool


def show_osd_pool_PG_distribution_txt(cdata):
    data, cols, sum_per_osd, sum_per_pool = calc_osd_pool_PG_distribution(cdata)
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


def show_summary(report, cdata):
    res = ["Status: " + cdata.s['health']['overall_status']]
    res.append("PG count: " + str(cdata.s['pgmap']['num_pgs']))
    res.append("Pool count: " + str(len(cdata.osd_lspools)))
    osd_count = len(cdata)
    res.append("OSD count: " + str(osd_count))
    res.append("PG per OSD: " + str(cdata.s['pgmap']['num_pgs'] / osd_count))
    res.append("Mon count: " + str(len(cdata.mon_status['monmap']['mons'])))
    report.divs.append("<br>\n".join(res) + "<br>\n")


def show_osd_devices_free_space(report, cdata):
    table = html.Table(header_row=["OSD", "status", "used GB", "free GB", "free %"])

    for osd_stats in sorted(cdata, key=lambda x: x.id):
        used = osd_stats.data_dev['used'] / 1024 ** 3
        free = osd_stats.data_dev['avail'] / 1024 ** 3

        free_perc = (osd_stats.data_dev['avail'] * 100) / \
            (osd_stats.data_dev['used'] + osd_stats.data_dev['avail'])

        if free_perc < 20:
            color = "red"
        elif free_perc < 40:
            color = "yellow"
        else:
            color = "green"

        free_perc = '<font color="{0}">{1}</font>'.format(color, free_perc)
        table.rows.append(map(str, [osd_stats.id, osd_stats.status, used, free, free_perc]))

    report.divs.append("<center><H3>OSD devices:</H3><br>\n" + str(table) + "</center>")


def show_pools_info(report, cdata):
    table = html.Table(header_row=["Pool", "size", "min_size", "crush_ruleset", "pg_count"])

    _, pools, _, sum_per_pool = calc_osd_pool_PG_distribution(cdata)

    for pool_name, data in sorted(cdata.pool_stats.items()):
        table.rows.append([pool_name,
                           str(data['size']),
                           str(data['min_size']),
                           str(data['crush_ruleset']),
                           str(sum_per_pool[pools.index(pool_name)])])

    report.divs.append("<center><H3>Pool stats:</H3><br>\n" + str(table) + "</center>")


def show_pg_state(report, cdata):
    statuses = collections.defaultdict(lambda: 0)
    for pg_group in cdata.s['pgmap']['pgs_by_state']:
        for state_name in pg_group['state_name'].split('+'):
            statuses[state_name] += pg_group["count"]

    keys, values = zip(*sorted(statuses.items()))
    table = html.Table(header_row=["sum"] + list(keys))
    table.rows.append([cdata.s['pgmap']['num_pgs']] + list(values))
    report.divs.append("<center><H3>PG status:</H3><br>\n" + str(table) + "</center>")


def show_osd_state(report, cdata):
    statuses = collections.defaultdict(lambda: [])

    for osd_stat in cdata:
        statuses[osd_stat.status].append(
            "{0.node}:{0.id}".format(osd_stat))

    table = html.Table(header_row=["Status", "Count", "ID's"])
    for status, nodes in sorted(statuses.items()):
        table.rows.append([status, len(nodes),
                           "" if status == "up" else ",".join(nodes)])
    report.divs.append("<center><H3>OSD state:</H3><br>\n" + str(table) + "</center>")


def show_osd_pool_PG_distribution_html(report, cdata):
    data, cols, sum_per_osd, sum_per_pool = calc_osd_pool_PG_distribution(cdata)
    table = html.Table(header_row=["OSD/pool"] + map(str, cols) + ['sum'])

    for name, row in data.items():
        idata = [row.get(i, 0) for i in cols]
        table.rows.append([str(name)] +
                          map(str, idata) +
                          [str(sum_per_osd[name])])

    table.rows.append(["sum"] +
                      map(str, sum_per_pool) +
                      [str(sum(sum_per_pool))])

    report.divs.append("<center><H3>PG per OSD:</H3><br>" + str(table) + "</center>")


visjs_script = """
  var network = null;
  function draw() {
    if (network !== null) network.destroy();
    var data = {nodes: [__nodes__], edges: [__eges__]};
    var options = {
      // layout: { hierarchical: {sortMethod: 'directed'}},
        edges: {smooth: true, arrows: {to : true }},
        nodes: {shape: 'dot'}
    };
    network = new vis.Network(document.getElementById('mynetwork'), data, options);
  }
"""

visjs_css = """
body {font: 10pt sans;}
#mynetwork {width: 100%;height: 800px;border: 1px solid lightgray;}
"""


def tree_to_visjs(report, cdata):
    report.style.append(visjs_css)
    report.style_links.append(
        "https://cdnjs.cloudflare.com/ajax/libs/vis/4.7.0/vis.min.css")
    report.script_links.append(
        "https://cdnjs.cloudflare.com/ajax/libs/vis/4.7.0/vis.min.js"
    )
    report.onload.append("draw()")
    report.divs.append('<div id="mynetwork"></div>')

    nodes_list = []
    eges_list = []
    nodes = cdata.osd_tree["nodes"]

    max_w = max(float(node['crush_weight'])
                for node in nodes
                if node['type'] == 'osd')

    min_w = min(float(node['crush_weight'])
                for node in nodes
                if node['type'] == 'osd')

    cmap = plt.get_cmap('rainbow')

    def get_color(node):
        if max_w - min_w < 1E-2 or node['type'] != 'osd':
            return "#ffffff"
        w = (float(node['crush_weight']) - min_w) / (max_w - min_w)
        return str(mcolors.rgb2hex(cmap(w)))

    if plt is not None:
        nodes_list = [
            "{{id:{0}, label:'{1}', color:'{2}'}}".format(
                node['id'],
                str(node['name']),
                get_color(node)
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

    nodes = ",".join(nodes_list)
    eges = ",".join(eges_list)

    report.scripts.append(
        visjs_script.replace('__nodes__', nodes).replace('__eges__', eges)
    )


node_templ_gv = '{0} [label="{1}"];'
link_templ_gv = '{0} -> {1};'


def getid(oid):
    return ("N" + str(oid) if oid >= 0 else "NN" + str(-oid))


def tree_to_graphviz(cdata):
    res = "digraph cluster{"
    nodes = cdata.osd_tree["nodes"]

    for node in nodes:
        res += node_templ_gv.format(getid(node['id']), str(node['name']))

    for node in nodes:
        for child_id in node.get('children', []):
            res += link_templ_gv.format(getid(node['id']), getid(child_id))

    return res + "}"


node_templ = ".addNode({{id: '{0}', label: '{1}', size: 1}})"
link_templ = ".addEdge({{id: 'e{0}', source: '{1}', target: '{2}'}})"


def tree_to_sigma(cdata):
    res = "s.graph"
    nodes = cdata.osd_tree["nodes"]

    for node in nodes:
        res += node_templ.format(getid(node['id']), node['name'])

    uniq_id = 0
    for node in nodes:
        for uniq_id, child_id in enumerate(node.get('children', []), uniq_id):
            res += link_templ.format(uniq_id, getid(node['id']), getid(child_id))

    return res


def main(argv):
    folder = argv[1]
    name = argv[2]
    remove_folder = False

    if os.path.isfile(folder):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            arch_name = folder
            folder = os.tempnam()
            os.makedirs(folder)
            remove_folder = True
            subprocess.call("tar -zxvf {0} -C {1} >/dev/null 2>&1".format(arch_name, folder), shell=True)

    if not os.path.isdir(folder):
        print "First argument should be a folder with data or path to archive"
        return 1

    try:
        cdata = ClusterData(folder)
        report = Report(name)
        # print show_osd_pool_PG_distribution(cdata)
        show_summary(report, cdata)
        show_osd_pool_PG_distribution_html(report, cdata)
        show_pools_info(report, cdata)
        show_osd_devices_free_space(report, cdata)
        show_pg_state(report, cdata)
        show_osd_state(report, cdata)
        tree_to_visjs(report, cdata)
        print str(report)
    finally:
        if remove_folder:
            shutil.rmtree(folder)


if __name__ == "__main__":
    exit(main(sys.argv))
