import sys
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
    ap("Used", b2ssize(cluster.bytes_used, False))
    ap("Avail", b2ssize(cluster.bytes_avail, False))
    ap("Data", b2ssize(cluster.data_bytes, False))

    avail_perc = cluster.bytes_avail * 100 / cluster.bytes_total
    ap("Free %", avail_perc)

    osd_count = len(cluster.osds)
    ap("Mon count", len(cluster.mons))

    report.divs.append(
        '<center><H3>Status:<br></H3><table border="0" cellpadding="5">' +
        "\n".join(res) +
        "</table></center>")

    del res[:]

    if cluster.settings is None:
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
        ap("Filestorage sync", str(cluster.settings.filestore_max_sync_interval) + 's')

    report.divs.append(
        '<center><H3>OSD:</H3><table border="0" cellpadding="5">' +
        "\n".join(res) +
        "</table></center>")
    del res[:]

    ap("Client IO Bps", b2ssize(cluster.write_bytes_sec, False))
    ap("Client IO IOPS", b2ssize(cluster.op_per_sec, False))

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

    report.divs.append("<center><H3>Monitors info:</H3><br>\n" + str(table) + "</center>")


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

    report.divs.append("<center><H3>PG's status:</H3><br>\n" + str(table) + "</center>")


def show_osd_state(report, cluster):
    statuses = collections.defaultdict(lambda: [])

    for osd in cluster.osds:
        statuses[osd.status].append("{0.host}:{0.id}".format(osd))

    table = html2.HTMLTable(headers=["Status", "Count", "ID's"])
    for status, osds in sorted(statuses.items()):
        table.add_row([status, len(osds),
                       "" if status == "up" else ",".join(osds)])
    report.divs.append("<center><H3>OSD's state:</H3><br>\n" + str(table) + "</center>")


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

    report.divs.append("<center><H3>Pool's stats:</H3><br>\n" + str(table) + "</center>")


HTML_UNKNOWN = '<font color="orange">???</font>'
html_ok = '<font color="green">{0}</font>'.format
html_fail = '<font color="red">{0}</font>'.format


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
                 "%.3f<br>%.3f" % (
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

            perf_info.append(os.path.basename(dev_stat['root_dev']))
            perf_info.append(b2ssize(dev_stat['read_bytes_uptime'], False))
            perf_info.append(b2ssize(dev_stat['write_bytes_uptime'], False))
            perf_info.append(b2ssize(dev_stat['read_iops_uptime'], False))
            perf_info.append(b2ssize(dev_stat['write_iops_uptime'], False))
            perf_info.append(int(dev_stat['io_time_uptime']))

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
                                     "D IO<br>time %",
                                     "J dev",
                                     "J read<br>Bps",
                                     "J write<br>Bps",
                                     "J read<br>IOOps",
                                     "J write<br>IOOps",
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
            perf_info.append(os.path.basename(dev_stat['root_dev']))
            perf_info.append(b2ssize(dev_stat['read_bytes_curr'], False))
            perf_info.append(b2ssize(dev_stat['write_bytes_curr'], False))
            perf_info.append(b2ssize(dev_stat['read_iops_curr'], False))
            perf_info.append(b2ssize(dev_stat['write_iops_curr'], False))
            perf_info.append(int(dev_stat['io_time_curr']))

        if have_data:
            table.add_row(map(str, [osd.id, osd.host] + perf_info))

    report.next_line()
    if have_any_data:
        report.divs.append("<center><H3>OSD's current load:</H3><br>\n" + str(table) + "</center>")
    else:
        report.divs.append("<center><H3>OSD's current load unawailable</H3></center><br>")


def show_host_network_load_in_color(report, cluster):
    net_io = collections.defaultdict(lambda: {})

    for host in cluster.hosts.values():
        ceph_adapters = [host.cluster_net.name, host.public_net.name]
        nets = [('cluster', host.cluster_net), ('public', host.public_net)]

        nets += [(net.name, net)
                 for net in host.net_adapters.values()
                 if net.is_phy and host.cluster_net not in ceph_adapters]

        for name, net in nets:
            if net is None or net.perf_stats_curr is None:
                continue

            usage = max((net.perf_stats_curr.sbytes,
                         net.perf_stats_curr.rbytes))

            if usage > 0:
                net_io[host.name][name] = (usage, net.speed)

    if len(net_io) == 0:
        report.divs.append("<center><H3>No current disk load awailable</H3></center><br>")
        return

    max_len = max(map(len, net_io.values()))

    table = '<table cellpadding="4" style="border: 1px solid #000000; border-collapse: collapse;"'
    table += 'border="1">'
    table += '<tr><th>host</th><th>public<br>net</th><th>cluster<br>net</th>'
    table += '<th>hw adapter</th>' * (max_len - 2) + "</tr>"

    for host_name, data in sorted(net_io.items()):
        table += "<tr><td>" + host_name + "</td>"
        for adapter, (usage, speed) in data.items():
            if speed is None:
                color = "#FFFFFF"
            else:
                color = val_to_color(usage / speed)
            table += '<td bgcolor="{0}"><b><font color="#303030">{1}: {2}</font></b></td>'\
                .format(color, adapter, b2ssize(usage, False))
        table += '<td />' * (max_len - len(data.items())) + "</tr>"

    table += "</table>"
    report.divs.append("<center><H3>Network load (to max dev throughput):</H3><br>\n" + table + "</center>")


def show_host_io_load_in_color(report, cluster):
    hosts_io_bytes = collections.defaultdict(lambda: {})
    hosts_io_iops = collections.defaultdict(lambda: {})
    hosts_io_wtime = collections.defaultdict(lambda: {})

    for osd in cluster.osds:
        for dev_stat in (osd.data_stor_stats, osd.j_stor_stats):
            if dev_stat is None or 'write_bytes_curr' not in dev_stat:
                continue

            dev = os.path.basename(dev_stat['root_dev'])
            hosts_io_bytes[osd.host][dev] = dev_stat['write_bytes_curr'] + dev_stat['read_bytes_curr']
            hosts_io_iops[osd.host][dev] = dev_stat['write_iops_curr'] + dev_stat['read_iops_curr']
            hosts_io_wtime[osd.host][dev] = int(dev_stat['w_io_time_curr'] * 100)

    if len(hosts_io_bytes) == 0:
        report.divs.append("<center><H3>No current IO load awailable</H3></center><br>")
        return

    loads = [
        (hosts_io_iops, 1000, 'iops'),
        (hosts_io_bytes, 1024, 'bps'),
        (hosts_io_wtime, 1000, 'util %'),
    ]

    for target, base, tp in loads:
        max_val = max(map(max, [data.values() for data in target.values()]))
        max_len = max(map(len, target.values()))

        table = '<table cellpadding="4" style="border: 1px solid #000000; border-collapse: collapse;"'
        table += ' border="1"><tr><th>host</th>'
        table += ('<th>load ' + tp + '</th>') * max_len + "</tr>"

        for host_name, data in sorted(target.items()):
            row = ""
            for dev, val in sorted(data.items()):
                if max_val == 0:
                    color = "#FFFFFF"
                else:
                    color = val_to_color(float(val) / max_val)

                row += '<td bgcolor="{0}"><b><font color="#303030">{1} {2}</font></b></td>'.format(
                    color, dev, b2ssize(val, False, base=base))

            table += "<tr><td>" + host_name + "</td>"
            table += row + '<td />' * (max_len - len(data.items())) + "</tr>"

        table += "</table>"
        report.divs.append("<center><H3>IO load (" + tp + "):</H3><br>\n" + table + "</center>")
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

    report.divs.append("<center><H3>Host's info:</H3><br>\n" + str(table) + "</center>")


def show_hosts_perf_stats(report, cluster):
    nets_info = {}

    for host in cluster.hosts.values():
        ceph_adapters = [host.cluster_net.name, host.public_net.name]
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

    report.divs.append("<center><H3>Host's resource usage:</H3><br>\n" + str(table) + "</center>")


def draw_resource_usage(report, cluster):
    script = ceph_report_template.body_script

    writes_per_dev = {}
    reads_per_dev = {}

    for osd in cluster.osds:

        perf_m = cluster.hosts[osd.host].perf_monitoring
        if perf_m is None or 'io' not in perf_m:
            continue

        if osd.data_stor_stats is not None and \
           osd.j_stor_stats is not None and \
           osd.j_stor_stats['root_dev'] == osd.data_stor_stats['root_dev']:
            dev_list = [('data/jornal', osd.data_stor_stats)]
        else:
            dev_list = [('data', osd.data_stor_stats),
                        ('journal', osd.j_stor_stats)]

        for tp, dev_stat in dev_list:
            if dev_stat is None:
                continue

            dev = os.path.basename(dev_stat['root_dev'])
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
            writes_per_dev[dev_uuid] = ','.join(map(str, writes))
            reads_per_dev[dev_uuid] = ','.join(map(str, reads))

    if len(writes_per_dev) != 0 or len(reads_per_dev) != 0:
        report.style.append(ceph_report_template.css)
        report.style.append(
            ".usage {width: 700px; height: 600px; border: 1px solid lightgray;}"
        )

        report.script_links.extend(ceph_report_template.scripts)

    if len(writes_per_dev) != 0:
        wdata_param = [
            '{0!r}: [{1}]'.format(str(dname), vals)
            for dname, vals in sorted(writes_per_dev.items())
        ]
        wall_devs = map(repr,
                        map(str,
                            sorted(writes_per_dev.keys())))

        div_id = 'io_w_usage'

        report.divs.append(
            '<center><H3>Disk writes:</H3></center><br>' +
            '<div id="' + div_id + '" class="usage" ></div><br>'
        )

        report.divs.append(
            script
            .replace('__id__', div_id)
            .replace('__devs__', ", ".join(wall_devs))
            .replace('__data__', ",".join(wdata_param))
        )

    if len(reads_per_dev) != 0:
        rdata_param = [
            '{0!r}: [{1}]'.format(str(dname), vals)
            for dname, vals in sorted(reads_per_dev.items())
        ]

        rall_devs = map(repr,
                        map(str,
                            sorted(reads_per_dev.keys())))

        div_id = 'io_r_usage'

        report.divs.append(
            '<br><center><H3>Disk reads:</H3></center><br>' +
            '<div id="' + div_id + '" class="usage" ></div><br>'
        )

        report.divs.append(
            script
            .replace('__id__', div_id)
            .replace('__devs__', ", ".join(rall_devs))
            .replace('__data__', ",".join(rdata_param))
        )


def show_osd_pool_PG_distribution_html(report, cluster):
    if cluster.sum_per_osd is None:
        report.divs.append("<center><H3>PG per OSD: No pg dump data. Probably too many PG</H3></center>")
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

    ncolor = [(v1 * dx2 + v2 * dx1 + 1) / 2 * 255 for v1, v2 in zip(color1, color2)]
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
        report.style.append('body {font: 10pt sans;}')

        show_summary(report, cluster)
        report.next_line()

        show_osd_pool_PG_distribution_html(report, cluster)
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

        tree_to_visjs(report, cluster)
        report.next_line()

        draw_resource_usage(report, cluster)
        report.next_line()

        if opts.out == '-':
            sys.stdout.write(str(report))
        else:
            open(opts.out, "w").write(str(report))
    finally:
        if remove_folder:
            shutil.rmtree(folder)


if __name__ == "__main__":
    exit(main(sys.argv))
