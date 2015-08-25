import sys
import json
import os.path
import texttable
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


class ClusterData(object):
    def __init__(self, folder):
        self.__folder = folder

    def __getattr__(self, name):
        try:
            fname = os.path.join(self.__folder, name) + ".json"
            val = json.loads(open(fname).read())
        except Exception as exc:
            raise AttributeError(str(exc))
        setattr(self, name, val)
        return val


def calc_osd_pool_PG_distribution(cdata):
    pool_id2name = dict((dt['poolnum'], dt['poolname']) for dt in cdata.lspools)

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


def show_osd_pool_PG_distribution_html(report, cdata):
    data, cols, sum_per_osd, sum_per_pool = calc_osd_pool_PG_distribution(cdata)
    table = html.Table(header_row=["OSD"] + map(str, cols) + ['sum'])

    for name, row in data.items():
        idata = [row.get(i, 0) for i in cols]
        table.rows.append([str(name)] +
                          map(str, idata) +
                          [str(sum_per_osd[name])])

    table.rows.append(["sum"] +
                      map(str, sum_per_pool) +
                      [str(sum(sum_per_pool))])

    report.divs.append(str(table))


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
    if os.path.isfile(folder):
        # unpack
        pass

    if not os.path.isdir(folder):
        print "First argument should be a folder with data or path to archive"
        return 1

    cdata = ClusterData(folder)
    report = Report(name)
    # print show_osd_pool_PG_distribution(cdata)
    show_osd_pool_PG_distribution_html(report, cdata)
    tree_to_visjs(report, cdata)
    print str(report)


if __name__ == "__main__":
    exit(main(sys.argv))
