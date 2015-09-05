import sys
import shutil
import bisect
import os.path
import warnings
import argparse
import subprocess
import collections

import html

from hw_info import b2ssize
from cluster import CephCluster
from storage import RawResultStorage, JResultStorage


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


def show_summary(report, cluster):
    res = []

    def ap(x, y):
        res.append('<tr><td>{0}:</td><td>{1}</td></tr>'.format(x, y))

    ap("Collected at", cluster.report_collected_at_local)
    ap("Collected at GMT", cluster.report_collected_at_gmt)
    ap("Status", cluster.overall_status)
    ap("PG count", cluster.num_pgs)
    ap("Pool count", len(cluster.pools))
    ap("Used GB", cluster.bytes_used / 1024 ** 3)
    ap("Avail GB", cluster.bytes_avail / 1024 ** 3)
    ap("Data GB", cluster.data_bytes / 1024 ** 3)

    avail_perc = cluster.bytes_avail * 100 / cluster.bytes_total
    ap("Free %", avail_perc)

    osd_count = len(cluster.osds)
    ap("Mon count", len(cluster.mons))

    report.divs.append(
        '<center><H3>Status:<br></H3><table border="0" cellpadding="5">' +
        "\n".join(res) +
        "</table></center>")

    del res[:]

    osd = cluster.get_alive_osd()

    if osd is None:
        res.append('<font color="red"><H3>No live OSD found!</H3></font><br>')
    else:
        ap("Count", osd_count)
        ap("PG per OSD", cluster.num_pgs / osd_count)
        ap("Cluster net", cluster.cluster_net)
        ap("Public net", cluster.public_net)
        ap("Near full ratio", cluster.settings.mon_osd_nearfull_ratio)
        ap("Full ratio", cluster.settings.mon_osd_full_ratio)
        ap("Backfill full ratio", cluster.settings.osd_backfill_full_ratio)
        ap("Filesafe full ratio", cluster.settings.osd_failsafe_full_ratio)
        ap("Journal aio", cluster.settings.journal_aio)
        ap("Journal dio", cluster.settings.journal_dio)

    report.divs.append(
        '<center><H3>OSD:</H3><table border="0" cellpadding="5">' +
        "\n".join(res) +
        "</table></center>")
    del res[:]

    ap("Client IO MBps",
        "%0.2f" % (cluster.write_bytes_sec / 2.0 ** 20))
    ap("Client IO IOPS", cluster.op_per_sec)

    report.divs.append(
        '<center><H3>Activity:</H3><table border="0" cellpadding="5">' +
        "\n".join(res) +
        "</table></center>")
    del res[:]

    report.next_line()

    if len(cluster.health_summary) != 0:
        messages = "<H3>Status messages:</H3><br>\n"
        for msg in cluster.health_summary:
            if msg['severity'] == "HEALTH_WARN":
                color = "orange"
            elif msg['severity'] == "HEALTH_ERR":
                color = "red"
            else:
                color = "black"

            messages += '<font color="{0}">{1}</font><br>\n'.format(color, msg['summary'])

        report.divs.append(messages)


def show_mons_info(report, cluster):
    table = html.Table(header_row=["Name",
                                   "Node",
                                   "Role",
                                   "Disk free<br>MB (%)"])

    for mon in cluster.mons:
        if mon.health == "HEALTH_OK":
            health = html_ok("HEALTH_OK")
        else:
            health = html_fail(mon.health)

        line = [
            mon.name,
            health,
            mon.role,
            "{0} ({1})".format(mon.kb_avail / 1024, mon.avail_percent)
        ]
        table.rows.append(map(str, line))

    report.divs.append("<center><H3>Monitors info:</H3><br>\n" + str(table) + "</center>")


def show_pg_state(report, cluster):
    statuses = collections.defaultdict(lambda: 0)
    for pg_group in cluster.pgmap_stat['pgs_by_state']:
        for state_name in pg_group['state_name'].split('+'):
            statuses[state_name] += pg_group["count"]

    npg = cluster.num_pgs
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
                 j_on_same_drive,
                 j_on_ssd,
                 j_on_file]))

    report.divs.append("<center><H3>OSD's info:</H3><br>\n" + str(table) + "</center>")


def show_osd_perf_info(report, cluster):
    table = html.Table(header_row=["OSD",
                                   "node",
                                   "apply<br>lat, ms",
                                   "commit<br>lat, ms",
                                   "D dev",
                                   "D read<br>Bps",
                                   "D write<br>Bps",
                                   "D read<br>IOOps",
                                   "D write<br>IOOps",
                                   "D IO time %",
                                   "J dev",
                                   "J read<br>Bps",
                                   "J write<br>Bps",
                                   "J read<br>IOOps",
                                   "J write<br>IOOps",
                                   "J IO time %",
                                   ])

    for osd in cluster.osds:
        if osd.osd_perf is not None:
            apply_latency_ms = osd.osd_perf["apply_latency_ms"]
            commit_latency_ms = osd.osd_perf["commit_latency_ms"]
        else:
            apply_latency_ms = HTML_UNKNOWN
            commit_latency_ms = HTML_UNKNOWN

        host = cluster.hosts[osd.host]
        perf_info = []

        if 'disk' in host.curr_perf_stats:
            start_time, start_data = host.curr_perf_stats['disk'][0]
            end_time, end_data = host.curr_perf_stats['disk'][-1]
            dtime = end_time - start_time

            for dev_stat in (osd.data_stor_stats, osd.j_stor_stats):
                if dev_stat is None:
                    perf_info.extend(['No data'] * 6)
                    continue

                dev = os.path.basename(osd.data_stor_stats['root_dev'])
                perf_info.append(dev)

                sd = start_data[dev]
                ed = end_data[dev]

                perf_info.append(b2ssize(float(ed.sectors_read - sd.sectors_read) * 512 / dtime, False))
                perf_info.append(b2ssize(float(ed.sectors_written - sd.sectors_written) * 512 / dtime, False))
                perf_info.append(b2ssize(int(ed.reads_completed - sd.reads_completed) / dtime, False))
                perf_info.append(b2ssize(int(ed.writes_completed - sd.writes_completed) / dtime, False))
                perf_info.append(int(0.1 * (ed.io_time - sd.io_time) / dtime))
        else:
            perf_info.extend(['No data'] * 12)

        table.rows.append(
            map(str,
                [osd.id,
                 osd.host,
                 apply_latency_ms,
                 commit_latency_ms] + perf_info))

    report.divs.append("<center><H3>OSD's performance info:</H3><br>\n" + str(table) + "</center>")


def show_hosts_stats(report, cluster):
    header_row = ["Hostname",
                  "Ceph services",
                  "CPU's",
                  "RAM<br>total",
                  "RAM<br>free",
                  "Swap<br>used"]
    table = html.Table(header_row=header_row)
    for host in sorted(cluster.hosts.values(), key=lambda x: x.name):
        services = ["osd-{0}".format(osd.id) for osd in cluster.osds if osd.host == host.name]
        all_mons = [mon.name for mon in cluster.mons]

        if host.name in all_mons:
            services.append("mon(" + host.name + ")")

        host_info = [host.name, "<br>".join(services)]

        if host.hw_info is None:
            table.rows.append(host_info + ['-'] * (header_row - len(host_info)))
            continue

        if host.hw_info.cores == []:
            host_info.append("Error")
        else:
            host_info.append(sum(count for _, count in host.hw_info.cores))

        host_info.append(b2ssize(host.mem_total))
        host_info.append(b2ssize(host.mem_free))
        host_info.append(b2ssize(host.swap_total - host.swap_free))
        table.rows.append(map(str, host_info))

    report.divs.append("<center><H3>Host's info:</H3><br>\n" + str(table) + "</center>")


def show_hosts_perf_stats(report, cluster):
    header_row = ["Hostname",
                  "Cluster net<br>dev, ip<br>settings",
                  "Cluster net<br>uptime average<br>send/recv Bps<br>send/recv pps",
                  "Cluster net<br>current<br>send/recv Bps<br>send/recv pps",
                  "Public net<br>dev, ip<br>settings",
                  "Public net<br>uptime average<br>send/recv Bps<br>send/recv pps",
                  "Public net<br>current<br>send/recv Bps<br>send/recv pps",
                  "Load avg<br>5 min"]
    table = html.Table(header_row=header_row)
    for host in sorted(cluster.hosts.values(), key=lambda x: x.name):
        perf_info = [host.name]
        for net in (host.cluster_net, host.public_net):
            if net is None:
                perf_info.append("No data")
                perf_info.append("No data")
            else:
                dev_ip = "{0}<br>{1}".format(net.adapter, net.ip)
                if net.adapter not in host.hw_info.net_info:
                    settings = "No data"
                else:
                    speed, dtype, _ = host.hw_info.net_info[net.adapter]
                    settings = "{0}, {1}".format(speed, dtype)

                perf_info.append("{0}<br>{1}".format(dev_ip, settings))

                perf_info.append("{0} / {1}<br>{2} / {3}".format(
                    b2ssize(float(net.perf_stats.sbytes) / host.uptime, False),
                    b2ssize(float(net.perf_stats.rbytes) / host.uptime, False),
                    b2ssize(float(net.perf_stats.spackets) / host.uptime, False),
                    b2ssize(float(net.perf_stats.rpackets) / host.uptime, False)
                ))

                if 'net' in host.curr_perf_stats:
                    start_time, start_data = host.curr_perf_stats['net'][0]
                    end_time, end_data = host.curr_perf_stats['net'][-1]
                    dtime = end_time - start_time

                    sd = start_data[net.adapter]
                    ed = end_data[net.adapter]

                    perf_info.append("{0} / {1}<br>{2} / {3}".format(
                        b2ssize(float(ed.sbytes - sd.sbytes) / dtime, False),
                        b2ssize(float(ed.rbytes - sd.rbytes) / dtime, False),
                        b2ssize(float(ed.spackets - sd.spackets) / dtime, False),
                        b2ssize(float(ed.rpackets - sd.rpackets) / dtime, False),
                    ))

                else:
                    perf_info.append('No data')

        perf_info.append(host.load_5m)
        table.rows.append(map(str, perf_info))

    report.divs.append("<center><H3>Host's perf info:</H3><br>\n" + str(table) + "</center>")


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


def tree_to_visjs(report, cluster):
    report.style.append(visjs_css)
    report.style_links.append(
        "https://cdnjs.cloudflare.com/ajax/libs/vis/4.7.0/vis.min.css")
    report.script_links.append(
        "https://cdnjs.cloudflare.com/ajax/libs/vis/4.7.0/vis.min.js"
    )

    # nodes = jstorage.master.osd_tree["nodes"]

    max_w = max(float(osd.crush_weight) for osd in cluster.osds)
    min_w = min(float(osd.crush_weight) for osd in cluster.osds)

    def get_color_w(node):
        if max_w - min_w < 1E-2 or node['type'] != 'osd':
            return "#ffffff"
        w = (float(node['crush_weight']) - min_w) / (max_w - min_w)
        return val_to_color(w)

    try:
        min_pg = min(cluster.sum_per_osd.values())
        max_pg = max(cluster.sum_per_osd.values())
    except AttributeError:
        min_pg = max_pg = None

    def get_color_pg_count(node):
        if cluster.sum_per_osd is None or min_pg is None or \
             (max_pg - min_pg) / float(max_pg) < 1E-2 or node['type'] != 'osd':

            return "#ffffff"

        w = (float(cluster.sum_per_osd[node['id']]) - min_pg) / (max_pg - min_pg)
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
            for node in cluster.osd_tree.values()
        ]

        for node in cluster.osd_tree.values():
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
    report.divs.append('<center><H3>Crush weight:</H3></center><br><div class="graph" id="mynetwork0"></div>')
    report.onload.append("draw0()")

    if cluster.sum_per_osd is not None:
        gnodes, geges = get_graph(get_color_pg_count)
        report.scripts.append(
            visjs_script.replace('__nodes__', gnodes)
                        .replace('__eges__', geges)
                        .replace('__id__', '1')
        )
        report.divs.append('<center><H3>PG\'s count:</H3></center><br><div class="graph" id="mynetwork1"></div>')
        report.onload.append("draw1()")
    report.next_line()


def parse_args(argv):
    p = argparse.ArgumentParser()
    p.add_argument("-o", '--out', help="output file", default='-')
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

        show_summary(report, cluster)
        report.next_line()

        show_osd_pool_PG_distribution_html(report, cluster)
        report.next_line()

        show_osd_info(report, cluster)
        report.next_line()

        show_osd_perf_info(report, cluster)
        report.next_line()

        show_pools_info(report, cluster)
        show_pg_state(report, cluster)
        report.next_line()

        show_osd_state(report, cluster)
        show_hosts_stats(report, cluster)
        show_mons_info(report, cluster)
        report.next_line()

        show_hosts_perf_stats(report, cluster)
        report.next_line()

        tree_to_visjs(report, cluster)

        if opts.out == '-':
            sys.stdout.write(str(report))
        else:
            open(opts.out, "w").write(str(report))
    finally:
        if remove_folder:
            shutil.rmtree(folder)


if __name__ == "__main__":
    exit(main(sys.argv))
