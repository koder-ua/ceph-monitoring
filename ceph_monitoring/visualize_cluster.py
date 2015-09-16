import sys
import json
import shutil
import pprint
import bisect
import os.path
import warnings
import argparse
import subprocess
import collections

import html2

from hw_info import b2ssize
import ceph_report_template
from cluster import CephCluster
from storage import RawResultStorage, JResultStorage


H = html2.rtag


def CH3(text):
    return H.center(H.H3(text))


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

<body{onload}>
    {divs}
</body></html>
"""


class Report(object):
    def __init__(self, cluster_name, output_file, report_template=default_report_templ):
        self.cluster_name = cluster_name
        self.output_file = output_file
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

    def save_to(self, output_dir):
        style = "\n".join(self.style)

        pt = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        static_files_dir = os.path.join(pt, "html_js_css")

        links = []
        for link in self.style_links + self.script_links:
            fname = link.rsplit('/', 1)[-1]
            src_path = os.path.join(static_files_dir, fname)
            dst_path = os.path.join(output_dir, fname)
            if os.path.exists(src_path):
                if not os.path.exists(dst_path):
                    shutil.copyfile(src_path, dst_path)
                link = fname
            links.append(link)

        css_links = links[:len(self.style_links)]
        js_links = links[len(self.style_links):]

        css_links_s = "\n".join(H.link('', href=url, rel="stylesheet", type="text/css")
                                for url in css_links)
        js_links_s = "\n".join(H.script('', type="text/javascript", src=url)
                               for url in js_links)
        scripts = "\n".join(H.script(script, type="text/javascript")
                            for script in self.scripts)

        onload = ";".join(self.onload)
        # divs = "<br>\n".join(self.divs)

        self.next_line()

        divs = []
        for div_line in self.div_lines:
            d = html2.Doc()
            with d.center.table('', border="0", cellpadding="20").tr:
                for obj in div_line[:-1]:
                    d.td(obj)
                    d.td('', width="40px")
                d.td(div_line[-1])

            divs.append(str(d) + "<br>")

        if onload != "":
            onload = ' onload="{0}"'.format(onload)

        index = self.template.format(
            cluster_name=self.cluster_name,
            style=style,
            css_links=css_links_s,
            script_links=js_links_s,
            scripts=scripts,
            onload=onload,
            divs="".join(divs)
        )

        index_path = os.path.join(output_dir, self.output_file)

        # try:
        #     import BeautifulSoup
        #     index = BeautifulSoup.BeautifulSoup(index).prettify()
        # except:
        #     pass

        open(index_path, "w").write(index)


def show_summary(report, cluster):
    t = html2.Doc()

    tsettings = {
        '_class': "table table-condensed table-bordered table-striped",
    }

    def ap(x, y):
        with t.tr:
            t.td(x)
            t.td(y)

    t.center.H3("Status:")
    with t.table('', **tsettings):
        ap("Collected at", cluster.report_collected_at_local)
        ap("Collected at GMT", cluster.report_collected_at_gmt)
        ap("Status", cluster.overall_status)
        ap("PG count", cluster.num_pgs)
        ap("Pool count", len(cluster.pools))
        ap("Used", b2ssize(cluster.bytes_used, False))
        ap("Avail", b2ssize(cluster.bytes_avail, False))
        ap("Data", b2ssize(cluster.data_bytes, False))

        avail_perc = cluster.bytes_avail * 100 / cluster.bytes_total
        ap("Free %", avail_perc)

        osd_count = len(cluster.osds)
        ap("Mon count", len(cluster.mons))

    report.divs.append(str(t))

    t = html2.Doc()

    def ap(x, y):
        with t.tr:
            t.td(x)
            t.td(y)

    t.center.H3("OSD:")
    if cluster.settings is None:
        t.font(color="red").H3("No live OSD found!<br>")
    else:
        with t.table('', **tsettings):
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
            ap("Filestorage sync", str(cluster.settings.filestore_max_sync_interval) + 's')
    report.divs.append(str(t))

    t = html2.Doc()

    def ap(x, y):
        with t.tr:
            t.td(x)
            t.td(y)

    t.center.H3("Activity:")
    with t.table('', **tsettings):
        ap("Client IO Bps", b2ssize(cluster.write_bytes_sec, False))
        ap("Client IO IOPS", b2ssize(cluster.op_per_sec, False))
    report.divs.append(str(t))

    report.next_line()

    if len(cluster.health_summary) != 0:
        messages = H.H3("Status messages:") + "<br>"
        for msg in cluster.health_summary:
            if msg['severity'] == "HEALTH_WARN":
                color = "orange"
            elif msg['severity'] == "HEALTH_ERR":
                color = "red"
            else:
                color = "black"

            messages += H.font(msg['summary'], color=color) + '<br>'

        report.divs.append(messages)


def show_mons_info(report, cluster):
    table = html2.HTMLTable(headers=["Name", "Node", "Role",
                                     "Disk free<br>B (%)"])

    for mon in cluster.mons:
        if mon.health == "HEALTH_OK":
            health = html_ok("HEALTH_OK")
        else:
            health = html_fail(mon.health)

        line = [
            mon.name,
            health,
            mon.role,
            "{0} ({1})".format(b2ssize(mon.kb_avail * 1024, False), mon.avail_percent)
        ]
        table.add_row(map(str, line))

    report.divs.append(CH3("Monitors info:") + str(table))


def show_pg_state(report, cluster):
    statuses = collections.defaultdict(lambda: 0)
    for pg_group in cluster.pgmap_stat['pgs_by_state']:
        for state_name in pg_group['state_name'].split('+'):
            statuses[state_name] += pg_group["count"]

    npg = cluster.num_pgs
    table = html2.HTMLTable(headers=["Status", "Count", "%"])
    table.add_row(["any", str(npg), "100.00"])
    for status, count in sorted(statuses.items()):
        table.add_row([status, str(count), "%.2f" % (100.0 * count / npg)])

    report.divs.append(CH3("PG's status:") + str(table))


def show_osd_state(report, cluster):
    statuses = collections.defaultdict(lambda: [])

    for osd in cluster.osds:
        statuses[osd.status].append("{0.host}:{0.id}".format(osd))

    table = html2.HTMLTable(headers=["Status", "Count", "ID's"])
    for status, osds in sorted(statuses.items()):
        table.add_row([status, len(osds),
                       "" if status == "up" else ",".join(osds)])
    report.divs.append(CH3("OSD's state:") + str(table))


def show_pools_info(report, cluster):
    table = html2.HTMLTable(headers=["Pool",
                                     "Id",
                                     "size",
                                     "min_size",
                                     "obj",
                                     "data",
                                     "free",
                                     "read",
                                     "write",
                                     "ruleset",
                                     "PG",
                                     "PGP"])

    for _, pool in sorted(cluster.pools.items()):
        vals = [pool.name,
                pool.id,
                pool.size,
                pool.min_size,
                b2ssize(int(pool.num_objects), base=1000),
                b2ssize(int(pool.size_bytes), False),
                '---',
                b2ssize(int(pool.read_bytes), False),
                b2ssize(int(pool.write_bytes), False),
                pool.crush_ruleset,
                pool.pg_num,
                pool.pg_placement_num]
        table.add_row(map(str, vals))

    report.divs.append(CH3("Pool's stats:") + str(table))


HTML_UNKNOWN = H.font('???', color="orange")


def html_ok(text):
    return H.font(text, color="green")


def html_fail(text):
    return H.font(text, color="red")


def show_osd_info(report, cluster):
    table = html2.HTMLTable(headers=["OSD",
                                     "node",
                                     "status",
                                     "daemon<br>run",
                                     "weight<br>reweight",
                                     "PG count",
                                     "Storage<br>used",
                                     "Storage<br>free",
                                     "Storage<br>free %",
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

            used = b2ssize(used_b, False)
            avail = b2ssize(avail_b, False)

            if avail_perc < 20:
                color = "red"
            elif avail_perc < 40:
                color = "yellow"
            else:
                color = "green"
            avail_perc_str = '<font color="{0}">{1}</font>'.format(color, avail_perc)

            if osd.data_stor_stats.root_dev == osd.j_stor_stats.root_dev:
                j_on_same_drive = html_fail("yes")
            else:
                j_on_same_drive = html_ok("no")

            if osd.data_stor_stats.dev != osd.j_stor_stats.dev:
                j_on_file = html_ok("no")
            else:
                j_on_file = html_fail("yes")

            if osd.j_stor_stats.is_ssd:
                j_on_ssd = html_ok("yes")
            else:
                j_on_ssd = html_fail("no")
        else:
            used = HTML_UNKNOWN
            avail = HTML_UNKNOWN
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

        table.add_row(
            map(str,
                [osd.id,
                 osd.host,
                 status,
                 daemon_msg,
                 "%.3f / %.3f" % (
                    float(osd.crush_weight),
                    float(osd.reweight)),
                 pg_count,
                 used,
                 avail,
                 avail_perc_str,
                 j_on_same_drive,
                 j_on_ssd,
                 j_on_file]))

    report.divs.append("<center><H3>OSD's info:</H3><br>\n" + str(table) + "</center>")


def show_osd_perf_info(report, cluster):
    table = html2.HTMLTable(headers=["OSD",
                                     "node",
                                     "apply<br>lat, ms",
                                     "commit<br>lat, ms",
                                     "D dev",
                                     "D read<br>Bps",
                                     "D write<br>Bps",
                                     "D read<br>IOOps",
                                     "D write<br>IOOps",
                                     "D IO<br>time %",
                                     "J dev",
                                     "J read<br>Bps",
                                     "J write<br>Bps",
                                     "J read<br>IOOps",
                                     "J write<br>IOOps",
                                     "J IO<br>time %",
                                     ])

    for osd in cluster.osds:
        if osd.osd_perf is not None:
            apply_latency_ms = osd.osd_perf["apply_latency_ms"]
            commit_latency_ms = osd.osd_perf["commit_latency_ms"]
        else:
            apply_latency_ms = HTML_UNKNOWN
            commit_latency_ms = HTML_UNKNOWN

        perf_info = []

        for dev_stat in (osd.data_stor_stats, osd.j_stor_stats):
            if dev_stat is None or 'read_bytes_uptime' not in dev_stat:
                perf_info.extend(['-'] * 6)
                continue

            perf_info.append(os.path.basename(dev_stat.root_dev))
            perf_info.append(b2ssize(dev_stat.read_bytes_uptime, False))
            perf_info.append(b2ssize(dev_stat.write_bytes_uptime, False))
            perf_info.append(b2ssize(dev_stat.read_iops_uptime, False))
            perf_info.append(b2ssize(dev_stat.write_iops_uptime, False))
            perf_info.append(int(dev_stat.io_time_uptime * 100))

        table.add_row(
            map(str,
                [osd.id,
                 osd.host,
                 apply_latency_ms,
                 commit_latency_ms] + perf_info))

    report.divs.append("<center><H3>OSD's load uptime average:</H3><br>\n" + str(table) + "</center>")

    table = html2.HTMLTable(headers=["OSD",
                                     "node",
                                     "D dev",
                                     "D read<br>Bps",
                                     "D write<br>Bps",
                                     "D read<br>IOOps",
                                     "D write<br>IOOps",
                                     "D lat<br>ms",
                                     "D IO<br>time %",
                                     "J dev",
                                     "J read<br>Bps",
                                     "J write<br>Bps",
                                     "J read<br>IOOps",
                                     "J write<br>IOOps",
                                     "J lat<br>ms",
                                     "J IO<br>time %",
                                     ])

    have_any_data = False
    for osd in cluster.osds:
        perf_info = []

        have_data = False
        for dev_stat in (osd.data_stor_stats, osd.j_stor_stats):
            if dev_stat is None or 'read_bytes_curr' not in dev_stat:
                perf_info.extend(['-'] * 6)
                continue

            have_data = True
            have_any_data = True
            perf_info.append(os.path.basename(dev_stat.root_dev))
            perf_info.append(b2ssize(dev_stat.read_bytes_curr, False))
            perf_info.append(b2ssize(dev_stat.write_bytes_curr, False))
            perf_info.append(b2ssize(dev_stat.read_iops_curr, False))
            perf_info.append(b2ssize(dev_stat.write_iops_curr, False))
            perf_info.append(int(dev_stat.lat_curr * 1000))
            perf_info.append(int(dev_stat.io_time_curr * 100))

        if have_data:
            table.add_row(map(str, [osd.id, osd.host] + perf_info))

    report.next_line()
    if have_any_data:
        report.divs.append(CH3("OSD's current load:") + str(table))
    else:
        report.divs.append(CH3("OSD's current load unawailable"))


def show_host_network_load_in_color(report, cluster):
    net_io = collections.defaultdict(lambda: {})
    send_net_io = collections.defaultdict(lambda: {})
    recv_net_io = collections.defaultdict(lambda: {})

    for host in cluster.hosts.values():
        ceph_adapters = []
        for net in (host.cluster_net, host.public_net):
            if net is not None:
                ceph_adapters.append(net.name)
            else:
                ceph_adapters.append('-')

        nets = [('cluster', host.cluster_net), ('public', host.public_net)]

        nets += [(net.name, net)
                 for net in host.net_adapters.values()
                 if net.is_phy and net.name not in ceph_adapters]

        for name, net in nets:
            if net is None or net.perf_stats_curr is None:
                continue

            usage = max((net.perf_stats_curr.sbytes,
                         net.perf_stats_curr.rbytes))

            if usage > 0 or name in ('cluster', 'public'):
                net_io[host.name][name] = (usage, net.speed)
                send_net_io[host.name][name] = (net.perf_stats_curr.sbytes, net.speed)
                recv_net_io[host.name][name] = (net.perf_stats_curr.rbytes, net.speed)

    if len(net_io) == 0:
        report.divs.append(CH3("No network load awailable"))
        return

    std_nets = set(['public', 'cluster'])

    loads = [
        # (net_io, "Network load max (to max dev throughput):"),
        (send_net_io, "Network load send (to max dev throughput):"),
        (recv_net_io, "Network load recv (to max dev throughput):"),
    ]

    for io, name in loads:
        max_len = max(len(set(data) - std_nets) for data in io.values())

        table = html2.HTMLTable(
            ["host", "public<br>net", "cluster<br>net"] + ["hw adapter"] * max_len
        )

        for host_name, data in sorted(io.items()):
            net_names = ['public', 'cluster'] + sorted(set(data) - set(['public', 'cluster']))

            table.add_cell(host_name)
            for net_name in net_names:
                if net_name not in data:
                    table.add_cell('-')
                    continue

                usage, speed = data[net_name]
                if speed is None:
                    color = "#FFFFFF"
                else:
                    color = val_to_color(float(usage) / speed)

                text = "{0}: {1}".format(net_name, b2ssize(usage, False))
                table.add_cell(H.font(text, color="#303030"), bgcolor=color)

            for i in range(max_len - len(data.items())):
                table.add_cell("")

            table.next_row()

        report.divs.append(CH3(name) + str(table))
        # report.next_line()


def show_host_io_load_in_color(report, cluster):
    rbts = collections.defaultdict(lambda: {})
    wbts = collections.defaultdict(lambda: {})
    bts = collections.defaultdict(lambda: {})

    wiops = collections.defaultdict(lambda: {})
    riops = collections.defaultdict(lambda: {})
    iops = collections.defaultdict(lambda: {})

    queue_depth = collections.defaultdict(lambda: {})
    lat = collections.defaultdict(lambda: {})
    io_time = collections.defaultdict(lambda: {})
    w_io_time = collections.defaultdict(lambda: {})

    for osd in cluster.osds:
        for dev_stat in (osd.data_stor_stats, osd.j_stor_stats):
            if dev_stat is None or 'write_bytes_curr' not in dev_stat:
                continue

            dev = os.path.basename(dev_stat.root_dev)

            wbts[osd.host][dev] = dev_stat.write_bytes_curr
            rbts[osd.host][dev] = dev_stat.read_bytes_curr
            bts[osd.host][dev] = dev_stat.write_bytes_curr + dev_stat.read_bytes_curr

            wiops[osd.host][dev] = dev_stat.write_iops_curr
            riops[osd.host][dev] = dev_stat.read_iops_curr
            iops[osd.host][dev] = dev_stat.write_iops_curr + dev_stat.read_iops_curr

            queue_depth[osd.host][dev] = dev_stat.w_io_time_curr
            lat[osd.host][dev] = dev_stat.lat_curr * 1000
            io_time[osd.host][dev] = dev_stat.io_time_curr
            w_io_time[osd.host][dev] = dev_stat.w_io_time_curr

    if len(wbts) == 0:
        report.divs.append(CH3("No current IO load awailable"))
        return

    loads = [
        (iops, 1000, 'iops', 50),
        (bts, 1024, 'bps', 200 * 1024),  # 50 IOPS * 4k

        (riops, 1000, 'read iops', 30),
        (wiops, 1000, 'write iops', 30),

        (rbts, 1024, 'read bps', 120 * 1024),  # 30 IOPS * 4k
        (wbts, 1024, 'write bps', 120 * 1024),  # 30 IOPS * 4k

        (queue_depth, None, 'io queue depts', 3),
        (lat, None, 'lat ms', 20),
        (io_time, None, 'io time', 1.0),
        (w_io_time, None, 'w io time', 5.0),
    ]

    columns = 0
    max_columns = 32
    for pos, (target, base, tp, min_max_val) in enumerate(loads, 1):
        if target is lat:
            max_val = 300.0
        else:
            max_val = max(map(max, [data.values() for data in target.values()]))
            max_val = max(min_max_val, max_val)

        max_len = max(map(len, target.values()))
        columns += max_len + 5  # public + cluster + headers * 2 + space

        if columns > max_columns and columns != max_len + 5:
            report.next_line()
            columns = max_len + 5

        table = html2.HTMLTable(['host'] + ['load'] * max_len)
        for host_name, data in sorted(target.items()):
            table.add_cell(host_name)
            for dev, val in sorted(data.items()):
                if max_val == 0:
                    color = "#FFFFFF"
                else:
                    color = val_to_color(float(val) / max_val)

                if target is lat:
                    s_val = str(int(val))
                elif target is queue_depth:
                    s_val = "%.1f" % (val,)
                elif base is None:
                    s_val = "%.1f" % (val,)
                else:
                    s_val = b2ssize(val, False, base=base)

                cell_data = H.font('{0} {1}'.format(dev, s_val), color="#303030")
                table.add_cell(cell_data, bgcolor=color)

            for i in range(max_len - len(data)):
                table.add_cell("")
            table.next_row()

        report.divs.append(
            CH3("IO load ({0}):".format(tp)) + str(table)
        )

    report.next_line()


def show_hosts_stats(report, cluster):
    header_row = ["Hostname",
                  "Ceph services",
                  "CPU's",
                  "RAM<br>total",
                  "RAM<br>free",
                  "Swap<br>used",
                  "Load avg<br>5m"]
    table = html2.HTMLTable(headers=header_row)
    for host in sorted(cluster.hosts.values(), key=lambda x: x.name):
        services = ["osd-{0}".format(osd.id) for osd in cluster.osds if osd.host == host.name]
        all_mons = [mon.name for mon in cluster.mons]

        if host.name in all_mons:
            services.append("mon(" + host.name + ")")

        host_info = [host.name, "<br>".join(services)]

        if host.hw_info is None:
            table.add_row(host_info + ['-'] * (len(header_row) - len(host_info)))
            continue

        if host.hw_info.cores == []:
            host_info.append("Error")
        else:
            host_info.append(sum(count for _, count in host.hw_info.cores))

        host_info.append(b2ssize(host.mem_total))
        host_info.append(b2ssize(host.mem_free))
        host_info.append(b2ssize(host.swap_total - host.swap_free))
        host_info.append(host.load_5m)
        table.add_row(map(str, host_info))

    report.divs.append(CH3("Host's info:") + str(table))


def show_hosts_perf_stats(report, cluster):
    nets_info = {}

    for host in cluster.hosts.values():
        ceph_adapters = [net.name
                         for net in (host.cluster_net, host.public_net)
                         if net is not None]
        nets = [net for net in host.net_adapters.values()
                if net.is_phy and host.cluster_net not in ceph_adapters]

        nets_info[host.name] = sorted(nets, key=lambda x: x.name)

    if len(nets_info) == 0:
        max_nets = 0
    else:
        max_nets = max(map(len, nets_info.values()))

    header_row = ["Hostname",
                  "Cluster net<br>dev, ip<br>settings",
                  "Cluster net<br>uptime average<br>send/recv Bps<br>send/recv pps",
                  "Cluster net<br>current<br>send/recv Bps<br>send/recv pps",
                  "Public net<br>dev, ip<br>settings",
                  "Public net<br>uptime average<br>send/recv Bps<br>send/recv pps",
                  "Public net<br>current<br>send/recv Bps<br>send/recv pps"]

    header_row += ["Net"] * max_nets
    row_len = len(header_row)

    table = html2.HTMLTable(headers=header_row)
    for host in sorted(cluster.hosts.values(), key=lambda x: x.name):
        perf_info = [host.name]

        for net in (host.cluster_net, host.public_net):
            if net is None:
                perf_info.extend(["-"] * 3)
            else:
                dev_ip = "{0}<br>{1}".format(net.name, net.ip)

                if host.hw_info is None or net.name not in host.hw_info.net_info:
                    settings = "-"
                else:
                    speed, dtype, _ = host.hw_info.net_info[net.name]
                    settings = "{0}, {1}".format(speed, dtype)

                perf_info.append("{0}<br>{1}".format(dev_ip, settings))

                perf_info.append("{0} / {1}<br>{2} / {3}".format(
                    b2ssize(float(net.perf_stats.sbytes) / host.uptime, False),
                    b2ssize(float(net.perf_stats.rbytes) / host.uptime, False),
                    b2ssize(float(net.perf_stats.spackets) / host.uptime, False),
                    b2ssize(float(net.perf_stats.rpackets) / host.uptime, False)
                ))

                if net.perf_stats_curr is not None:
                    perf_info.append("{0} / {1}<br>{2} / {3}".format(
                        b2ssize(net.perf_stats_curr.sbytes, False),
                        b2ssize(net.perf_stats_curr.rbytes, False),
                        b2ssize(net.perf_stats_curr.spackets, False),
                        b2ssize(net.perf_stats_curr.rpackets, False),
                    ))
                else:
                    perf_info.append('-')

        for net in nets_info[host.name]:
            if net.speed is not None:
                perf_info.append(net.name + ": " + b2ssize(net.speed))
            else:
                perf_info.append(net.name)
        perf_info += ['-'] * (row_len - len(perf_info))

        table.add_row(map(str, perf_info))

    report.divs.append(CH3("Host's resource usage:") + str(table))


def get_io_resource_usage(cluster):
    writes_per_dev = {}
    reads_per_dev = {}
    order = []

    for osd in sorted(cluster.osds, key=lambda x: x.id):
        perf_m = cluster.hosts[osd.host].perf_monitoring
        if perf_m is None or 'io' not in perf_m:
            continue

        if osd.data_stor_stats is not None and \
           osd.j_stor_stats is not None and \
           osd.j_stor_stats.root_dev == osd.data_stor_stats.root_dev:
            dev_list = [('data/jornal', osd.data_stor_stats)]
        else:
            dev_list = [('data', osd.data_stor_stats),
                        ('journal', osd.j_stor_stats)]

        for tp, dev_stat in dev_list:
            if dev_stat is None:
                continue

            dev = os.path.basename(dev_stat.root_dev)
            if dev not in perf_m['io']:
                continue

            prev_val = perf_m['io'][dev].values[0]
            writes = []
            reads = []
            for val in perf_m['io'][dev].values[1:]:
                writes.append(val.writes_completed - prev_val.writes_completed)
                reads.append(val.reads_completed - prev_val.reads_completed)
                prev_val = val

            dev_uuid = "osd-{0}.{1}".format(str(osd.id), tp)
            writes_per_dev[dev_uuid] = writes
            reads_per_dev[dev_uuid] = reads
            order.append(dev_uuid)
    return writes_per_dev, reads_per_dev, order


def draw_resource_usage(report, cluster):
    script = ceph_report_template.body_script

    writes_per_dev, reads_per_dev, order = get_io_resource_usage(cluster)

    for dct in (writes_per_dev, reads_per_dev):
        for key in list(dct):
            dct[key] = ','.join(map(str, dct[key]))

    if len(writes_per_dev) != 0 or len(reads_per_dev) != 0:
        report.style.append(ceph_report_template.css)
        report.style.append(
            ".usage {width: 700px; height: 600px; border: 1px solid lightgray;}"
        )

        report.script_links.extend(ceph_report_template.scripts)

    if len(writes_per_dev) != 0:
        wdata_param = [
            '{0!r}: [{1}]'.format(str(dname), writes_per_dev[dname])
            for dname in order
        ]
        wall_devs = map(repr, order)

        div_id = 'io_w_usage'

        report.divs.append(CH3("Disk writes:") + H.div('', id=div_id))

        report.divs.append(
            script
            .replace('__id__', div_id)
            .replace('__devs__', ", ".join(wall_devs))
            .replace('__data__', ",".join(wdata_param))
        )

    if len(reads_per_dev) != 0:
        rdata_param = [
            '{0!r}: [{1}]'.format(str(dname), reads_per_dev[dname])
            for dname in order
        ]
        rall_devs = map(repr, order)

        div_id = 'io_r_usage'

        report.divs.append(CH3("Disk reads:") + H.div('', id=div_id))

        report.divs.append(
            script
            .replace('__id__', div_id)
            .replace('__devs__', ", ".join(rall_devs))
            .replace('__data__', ",".join(rdata_param))
        )


rickshaw_js_links = [
    "https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.6/d3.min.js",
    "https://cdnjs.cloudflare.com/ajax/libs/rickshaw/1.5.1/rickshaw.min.js"
]

rickshaw_code = """
<script>
var __graph_var_name__ = new Rickshaw.Graph( {
    element: document.querySelector("#__div_id__"),
    width: __width__,
    height: __height__,
    renderer: 'line',
    series: [
        __series__
    ]
});

__graph_var_name__.render();
</script>
"""


def draw_resource_usage_rsw(report, cluster):
    report.script_links.extend(rickshaw_js_links)
    writes_per_dev, reads_per_dev, order = get_io_resource_usage(cluster)

    if len(writes_per_dev) != 0:
        series = {}
        for dname, vals in writes_per_dev.items():
            series[dname] = [{'x': pos, 'y': val}
                             for pos, val in enumerate(vals)]

        plots_per_div = 2
        for dev in range(0, len(order), plots_per_div):
            div_id = "rsw_{0}".format(dev)
            var_name = "rsw_var_{0}".format(dev)

            data = []
            for name in order[dev: dev + plots_per_div]:
                data.append({
                    'color': "#c05020",
                    'data': series[name],
                    'name': name
                })

            json_data = ",\n".join(map(json.dumps, data))

            report.divs.append("New load for " + ",".join(order[dev: dev + plots_per_div]))
            report.divs.append(H.div('', id=div_id, _class="usage"))
            report.divs.append(
                rickshaw_code
                .replace('__div_id__', div_id)
                .replace('__graph_var_name__', var_name)
                .replace('__width__', '600')
                .replace('__height__', '80')
                .replace('__color__', 'green')
                .replace('__series__', json_data) + "\n"
            )
            report.next_line()


def show_osd_pool_PG_distribution_html(report, cluster):
    if cluster.sum_per_osd is None:
        report.divs.append(CH3("PG per OSD: No pg dump data. Probably too many PG"))
        return

    pools = sorted(cluster.sum_per_pool)
    table = html2.HTMLTable(headers=["OSD/pool"] + list(pools) + ['sum'])

    for osd_id, row in sorted(cluster.osd_pool_pg_2d.items()):
        data = [osd_id] + \
               [row.get(pool_name, 0) for pool_name in pools] + \
               [cluster.sum_per_osd[osd_id]]

        table.add_row(map(str, data))

    table.add_row(["sum"] +
                  [cluster.sum_per_pool[pool_name] for pool_name in pools] +
                  [str(sum(cluster.sum_per_pool.values()))])

    report.divs.append(CH3("PG per OSD:") + str(table))


def show_pool_PG_dispersion_html(report, cluster):
    if cluster.sum_per_osd is None:
        report.divs.append(CH3("PG per OSD: No pg dump data. Probably too many PG"))
        return

    dev_per_pool = {}
    for pool_name in cluster.sum_per_pool:
        row = [osd_pg.get(pool_name, 0)
               for osd_pg in cluster.osd_pool_pg_2d.values()]
        avg = float(sum(row)) / len(row)
        dev = (sum((i - avg) ** 2.0 for i in row) / len(row)) ** 0.5
        dev_per_pool[pool_name] = int(dev * 100. / avg)

    table = html2.HTMLTable(headers=["pool", "dispersion %"])

    for pool, dev in sorted(dev_per_pool.items()):
        table.add_row([pool, str(int(dev))])

    report.divs.append(CH3("Pool PG dispersion:") + str(table))


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
        ncolor = color_map[0][1]
    elif pos >= len(idx):
        ncolor = color_map[-1][1]
    else:
        color1 = color_map[pos - 1][1]
        color2 = color_map[pos][1]

        dx1 = (val - idx[pos - 1]) / (idx[pos] - idx[pos - 1])
        dx2 = (idx[pos] - val) / (idx[pos] - idx[pos - 1])

        ncolor = [(v1 * dx2 + v2 * dx1)
                  for v1, v2 in zip(color1, color2)]

    ncolor = [(channel * 255 + 255) / 2 for channel in ncolor]
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
        min_pg = min(max_pg / 2, min_pg)
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
    report.divs.append(CH3('Crush weight:') + H.div(_class="graph", id="mynetwork0"))
    report.onload.append("draw0()")

    if cluster.sum_per_osd is not None:
        gnodes, geges = get_graph(get_color_pg_count)
        report.scripts.append(
            visjs_script.replace('__nodes__', gnodes)
                        .replace('__eges__', geges)
                        .replace('__id__', '1')
        )
        report.divs.append(CH3('PG\'s count:') +
                           H.div('', _class="graph", id="mynetwork1"))
        report.onload.append("draw1()")
    report.next_line()


def parse_args(argv):
    p = argparse.ArgumentParser()
    p.add_argument("-o", '--out',
                   help="report output folder", required=True)
    p.add_argument("-w", '--overwrite', action='store_true', default=False,
                   help="Overwrite result folder data")
    p.add_argument("-n", "--name", help="Report name", default="Nemo")
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

    index_path = os.path.join(opts.out, 'index.html')
    if os.path.exists(index_path):
        if not opts.overwrite:
            print index_path, "already exists. Exits"
            return 1
    elif not os.path.exists(opts.out):
        os.makedirs(opts.out)

    try:
        storage = RawResultStorage(folder)
        jstorage = JResultStorage(storage)

        cluster = CephCluster(jstorage, storage)
        cluster.load()

        report = Report(opts.name, "index.html")
        report.style.append('body {font: 10pt sans;}')
        # report.style.append("table.colored tbody tr:nth-child(2n) td {background: #ccccff;}")
        # report.style.append("table.colored tbody tr:nth-child(2n+1) td {background: #ffcccc;}")
        report.style_links.append("https://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap.min.css")
        report.script_links.append("http://www.kryogenix.org/code/browser/sorttable/sorttable.js")

        show_summary(report, cluster)
        report.next_line()

        show_osd_pool_PG_distribution_html(report, cluster)
        show_pool_PG_dispersion_html(report, cluster)
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

        show_host_io_load_in_color(report, cluster)
        report.next_line()

        show_host_network_load_in_color(report, cluster)
        report.next_line()

        show_hosts_perf_stats(report, cluster)
        report.next_line()
        report.save_to(opts.out)
        print "Report successfully stored in", index_path

        # tree_to_visjs(report, cluster)
        # report.next_line()

        # draw_resource_usage(report, cluster)
        # report.next_line()

        perf_path = os.path.join(opts.out, "performance.html")
        load_report = Report(opts.name, "performance.html")
        draw_resource_usage_rsw(load_report, cluster)
        load_report.save_to(opts.out)
        print "Peformance report successfully stored in", perf_path
    finally:
        if remove_folder:
            shutil.rmtree(folder)


if __name__ == "__main__":
    exit(main(sys.argv))
