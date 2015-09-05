import re
import collections

from ipaddr import IPNetwork, IPAddress
from hw_info import get_hw_info, ssize2b


class CephOSD(object):
    def __init__(self):
        self.id = None
        self.status = None
        self.host = None
        self.daemon_runs = None
        self.pg_count = None
        self.config = None

        self.data_stor_stats = None
        self.j_stor_stats = None


class CephMonitor(object):
    def __init__(self):
        self.name = None
        self.status = None
        self.host = None
        self.role = None


class Pool(object):
    def __init__(self):
        self.id = None
        self.name = None


class HostNetworkInfo(object):
    def __init__(self, adapter, ip):
        self.adapter = adapter
        self.ip = ip
        self.perf_stats = None
        self.perf_delta = None


class Disk(object):
    def __init__(self, dev):
        self.dev = dev
        self.perf_stats = None
        self.perf_delta = None


class Host(object):
    def __init__(self, name):
        self.name = name
        self.cluster_net = None
        self.public_net = None
        self.net_adapters = {}
        self.disks = {}
        self.uptime = None


class TabulaRasa(object):
    pass


DiskStats = collections.namedtuple(
    "DiskStats",
    ["major",
     "minor",
     "device",
     "reads_completed",
     "reads_merged",
     "sectors_read",
     "read_time",
     "writes_completed",
     "writes_merged",
     "sectors_written",
     "write_time",
     "in_progress_io",
     "io_time",
     "weighted_io_time"]
)


NetStats = collections.namedtuple(
    "NetStats",
    ("rbytes rpackets rerrs rdrop rfifo rframe rcompressed" +
     " rmulticast sbytes spackets serrs sdrop sfifo scolls" +
     " scarrier scompressed").split()
)


def parse_netdev(netdev):
    info = {}
    for line in netdev.strip().split("\n")[2:]:
        adapter, data = line.split(":")
        assert adapter not in info
        info[adapter] = NetStats(*map(int, data.split()))

    return info


def parse_diskstats(diskstats):
    info = {}
    for line in diskstats.strip().split("\n"):
        data = line.split()
        data_i = map(int, data[:2]) + [data[2]] + map(int, data[3:])
        info[data[2]] = DiskStats(*data_i)
    return info


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
        self.hosts = {}

        self.osd_tree = {}
        self.osd_tree_root_id = None
        self.report_collected_at_local = None
        self.report_collected_at_gmt = None

        self.cluster_net = None
        self.public_net = None

        self.storage = storage
        self.jstorage = jstorage
        self.settings = TabulaRasa()

    def get_alive_osd(self):
        # try to find alive osd
        for osd in self.osds:
            if osd.status == 'up' and osd.daemon_runs:
                return osd
        return None

    def load(self):
        self.load_osd_tree()
        self.load_PG_distribution()
        self.load_osds()
        self.load_cluster_networks()
        self.load_pools()
        self.load_monitors()
        self.load_hosts()

        for host in self.hosts.values():
            host.curr_perf_stats = self.get_perf_stats(host.name)

        data = self.storage.get('master/collected_at')
        assert data is not None
        self.report_collected_at_local, \
            self.report_collected_at_gmt, _ = data.strip().split("\n")

        mstorage = self.jstorage.master

        self.overall_status = mstorage.status['health']['overall_status']
        self.health_summary = mstorage.status['health']['summary']
        self.num_pgs = mstorage.status['pgmap']['num_pgs']

        self.bytes_used = mstorage.status['pgmap']["bytes_used"]
        self.bytes_total = mstorage.status['pgmap']["bytes_total"]
        self.bytes_avail = mstorage.status['pgmap']["bytes_avail"]
        self.data_bytes = mstorage.status['pgmap']["data_bytes"]
        self.write_bytes_sec = mstorage.status['pgmap'].get("write_bytes_sec", 0)
        self.op_per_sec = mstorage.status['pgmap'].get("op_per_sec", 0)

        for osd in self.osds:
            if osd.status == 'up':
                self.settings.__dict__.update(osd.config)
                break

        self.pgmap_stat = mstorage.status['pgmap']

    def load_cluster_networks(self):
        self.cluster_net = None
        self.public_net = None

        osd = self.get_alive_osd()
        if osd is not None:
            cluster_net_str = osd.config.get('cluster_network')
            if cluster_net_str is not None:
                self.cluster_net = IPNetwork(cluster_net_str)

            public_net_str = osd.config.get('public_network', None)
            if public_net_str is not None:
                self.public_net = IPNetwork(public_net_str)

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

        # set hosts
        def fill_host(obj, host=None):
            obj['host'] = host
            if obj['type'] == 'host':
                host = obj['name']
            if 'children' in obj:
                for child_id in obj['children']:
                    fill_host(self.osd_tree[child_id], host)

        fill_parent(self.osd_tree[self.osd_tree_root_id])
        fill_host(self.osd_tree[self.osd_tree_root_id])

    def find_host_for_node(self, node):
        cnode = node
        while cnode['type'] != 'host':
            if cnode['parent'] is None:
                raise IndexError("Can't found host for " + str(node['id']))
            cnode = self.osd_tree[cnode['parent']]
        return cnode

    def load_osds(self):
        for node in self.osd_tree.values():
            if node['type'] != 'osd':
                continue

            osd = CephOSD()
            self.osds.append(osd)
            osd.__dict__.update(node)
            osd.host = node['host']

            try:
                osd_data = getattr(self.jstorage.osd, str(node['id']))
                osd.data_stor_stats = osd_data.data.stats
                osd.j_stor_stats = osd_data.journal.stats
            except AttributeError:
                osd.data_stor_stats = None
                osd.j_stor_stats = None

            osd.osd_perf = find(self.jstorage.master.osd_perf["osd_perf_infos"],
                                lambda x: x['id'] == osd.id)["perf_stats"]

            data = self.storage.get('osd/{0}/osd_daemons'.format(osd.id))
            if data is None:
                osd.daemon_runs = None
            else:
                for line in data.split("\n"):
                    if 'ceph-osd' in line and '-i {0}'.format(osd.id) in line:
                        osd.daemon_runs = True
                        break
                else:
                    osd.daemon_runs = False

            if self.sum_per_osd is not None:
                osd.pg_count = self.sum_per_osd[osd.id]
            else:
                osd.pg_count = None

            try:
                osd.config = self.jstorage.osd.get("{0}/config".format(osd.id))
            except AttributeError:
                pass

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

    def load_monitors(self):
        srv_health = self.jstorage.master.status['health']['health']['health_services']
        assert len(srv_health) == 1
        for srv in srv_health[0]['mons']:
            mon = CephMonitor()
            mon.health = srv["health"]
            mon.name = srv["name"]
            mon.host = srv["name"]
            mon.kb_avail = srv["kb_avail"]
            mon.avail_percent = srv["avail_percent"]
            self.mons.append(mon)

    def get_node_net_stats(self, host_name):
        return parse_netdev(self.storage.get('hosts/{0}/netdev'.format(host_name)))

    def get_node_disk_stats(self, host_name):
        return parse_netdev(self.storage.get('hosts/{0}/diskstats'.format(host_name)))

    def load_PG_distribution(self):
        try:
            pg_dump = self.jstorage.master.pg_dump
        except AttributeError:
            self.osd_pool_pg_2d = None
            self.sum_per_pool = None
            self.sum_per_osd = None
            return

        pool_id2name = dict((dt['poolnum'], dt['poolname'])
                            for dt in self.jstorage.master.osd_lspools)

        self.osd_pool_pg_2d = collections.defaultdict(lambda: collections.Counter())
        self.sum_per_pool = collections.Counter()
        self.sum_per_osd = collections.Counter()

        for pg in pg_dump['pg_stats']:
            pool = int(pg['pgid'].split('.', 1)[0])
            for osd_num in pg['acting']:
                pool_name = pool_id2name[pool]
                self.osd_pool_pg_2d[osd_num][pool_name] += 1
                self.sum_per_pool[pool_name] += 1
                self.sum_per_osd[osd_num] += 1

    def parse_meminfo(self, meminfo):
        info = {}
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

    def load_hosts(self):
        for json_host in self.osd_tree.values():
            if json_host["type"] != "host":
                continue

            stor_node = self.storage.get("hosts/" + json_host['name'], expected_format=None)

            host = Host(json_host['name'])
            self.hosts[host.name] = host

            try:
                lshw_xml = stor_node.get('lshw', expected_format='xml')
            except AttributeError:
                host.hw_info = None
            else:
                host.hw_info = get_hw_info(lshw_xml)

            info = self.parse_meminfo(stor_node.get('meminfo'))
            host.mem_total = info['MemTotal']
            host.mem_free = info['MemFree']
            host.swap_total = info['SwapTotal']
            host.swap_free = info['SwapFree']
            loadavg = stor_node.get('loadavg')

            host.load_5m = None if loadavg is None else float(loadavg.strip().split()[1])

            ipa = self.storage.get('hosts/%s/ipa' % host.name)
            ip_rr_s = r"\d+:\s+(?P<adapter>.*?)\s+inet\s+(?P<ip>\d+\.\d+\.\d+\.\d+)/(?P<size>\d+)"

            info = collections.defaultdict(lambda: [])
            for line in ipa.split("\n"):
                match = re.match(ip_rr_s, line)
                if match is not None:
                    info[match.group('adapter')].append(
                        (IPAddress(match.group('ip')), int(match.group('size'))))

            for adapter, ips_with_sizes in info.items():
                for ip, sz in ips_with_sizes:
                    if ip in self.public_net:
                        host.public_net = HostNetworkInfo(adapter, ip)

                    if ip in self.cluster_net:
                        host.cluster_net = HostNetworkInfo(adapter, ip)

            net_stats = self.get_node_net_stats(host.name)
            for net in (host.cluster_net, host.public_net):
                if net.adapter is not None:
                    net.perf_stats = net_stats.get(net.adapter)

            host.uptime = float(stor_node.get('uptime').split()[0])

    def get_perf_stats(self, host_name):
        stats = collections.defaultdict(lambda: [])
        host_stats = self.storage.get("perf_stats/" + host_name, expected_format=None)
        for stat_name in host_stats:
            collect_time, stat_type = stat_name.split("-")

            if stat_type == 'disk':
                stat = parse_diskstats(host_stats.get(stat_name))
            elif stat_type == 'net':
                stat = parse_netdev(host_stats.get(stat_name))
            else:
                raise ValueError("Unknown stat type - {!r}".format(stat_type))

            stats[stat_type].append([int(collect_time), stat])

        for stat_list in stats.values():
            stat_list.sort()

        return stats
