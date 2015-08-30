import re
import sys
import time
import json
import Queue
import shutil
import logging
import os.path
import argparse
import warnings
import threading
import subprocess
import collections


logger = logging.getLogger('collect')


class CollectSettings(object):
    def __init__(self):
        self.disabled = []

    def disable(self, pattern):
        self.disabled.append(re.compile(pattern))

    def allowed(self, path):
        for pattern in self.disabled:
            if pattern.search(path):
                return False
        return True


def check_output(cmd, log=True):
    if log:
        logger.debug("CMD: %r", cmd)

    p = subprocess.Popen(cmd, shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    out = p.communicate()
    code = p.wait()
    return code == 0, out[0]


def check_output_ssh(host, opts, cmd):
    logger.debug("SSH:%s: %r", host, cmd)
    ssh_opts = "-o LogLevel=quiet -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    return check_output("ssh {2} {0} {1}".format(host, cmd, ssh_opts), False)


class Collector(object):
    name = None
    run_alone = False

    def __init__(self, opts, collect_settings, res_q):
        self.collect_settings = collect_settings
        self.opts = opts
        self.res_q = res_q

    def run2emit(self, path, format, cmd, check=True):
        if check:
            if not self.collect_settings.allowed(path):
                return
        ok, out = check_output(cmd)
        if not ok:
            logger.warning("Cmd {0} failed locally".format(cmd))
        self.emit(path, format, ok, out, check=False)

    def ssh2emit(self, host, path, format, cmd, check=True):
        if check:
            if not self.collect_settings.allowed(path):
                return
        ok, out = check_output_ssh(host, self.opts, cmd)
        if not ok:
            logger.warning("Cmd {0} failed on node {1}".format(cmd, host))
        self.emit(path, format, ok, out, check=False)

    def emit(self, path, format, ok, out, check=True):
        if check:
            if not self.collect_settings.allowed(path):
                return
        self.res_q.put((ok, path, (format if ok else 'err'), out))

    # should provides set of on_XXX methods
    # where XXX - node role role
    # def collect_XXX(self, path, node, **params):
    #    pass


class CephDataCollector(Collector):

    name = 'ceph'
    run_alone = False

    def __init__(self, *args, **kwargs):
        Collector.__init__(self, *args, **kwargs)
        self.ceph_cmd = "ceph -c {0.conf} -k {0.key} --format json ".format(self.opts)

    def collect_master(self, path=None, node=None):
        path = path + "/master/"

        for cmd in ['osd tree', 'pg dump', 'df', 'auth list',
                    'health', 'health detail', "mon_status",
                    'status']:
            self.run2emit(path + cmd.replace(" ", "_"), 'json',
                          self.ceph_cmd + cmd)

        self.run2emit(path + "rados_df", 'json',
                      "rados df -c {0.conf} -k {0.key} --format json".format(self.opts))

        ok, lspools = check_output(self.ceph_cmd + "osd lspools")
        self.emit(path + "osd_lspools", 'json', ok, lspools)
        assert ok

        pool_stats = {}
        for pool in json.loads(lspools):
            pool_name = pool['poolname']
            pool_stats[pool_name] = {}
            for stat in ['size', 'min_size', 'crush_ruleset']:
                ok, val = check_output(self.ceph_cmd + "osd pool get {0} {1}".format(pool_name, stat))
                assert ok
                pool_stats[pool_name][stat] = json.loads(val)[stat]

        self.emit(path + 'pool_stats', 'json', True, json.dumps(pool_stats))

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out_file = os.tempnam()
            ok, out = check_output(self.ceph_cmd + "osd getcrushmap -o " + out_file)
            if not ok:
                self.emit(path + 'crushmap', 'err', ok, out)
            else:
                data = open(out_file, "rb").read()
                os.unlink(out_file)
                self.emit(path + 'crushmap', 'bin', ok, data)

    def emit_device_info(self, host, path, device_file):
        ok, dev_str = check_output_ssh(host, self.opts, "df " + device_file)
        assert ok

        dev_str = dev_str.strip()
        dev_link = dev_str.strip().split("\n")[1].split()[0]

        if dev_link == 'udev':
            dev_link = device_file

        used = int(dev_str.strip().split("\n")[1].split()[2]) * 1024
        avail = int(dev_str.strip().split("\n")[1].split()[3]) * 1024

        abs_path_cmd = '\'path="{0}" ;'.format(dev_link)
        abs_path_cmd += 'while [ -h "$path" ] ; do path=$(readlink "$path") ;'
        abs_path_cmd += ' path=$(readlink -f "$path") ; done ; echo $path\''
        ok, dev = check_output_ssh(host, self.opts, abs_path_cmd)
        assert ok

        root_dev = dev = dev.strip()
        while root_dev[-1].isdigit():
            root_dev = root_dev[:-1]

        cmd = "cat /sys/block/{0}/queue/rotational".format(os.path.basename(root_dev))
        ok, is_ssd_str = check_output_ssh(host, self.opts, cmd)
        assert ok
        is_ssd = is_ssd_str.strip() == '0'

        self.ssh2emit(host, path + '/hdparm', 'txt', "sudo hdparm -I " + root_dev)
        self.ssh2emit(host, path + '/smartctl', 'txt', "sudo smartctl -a " + root_dev)
        self.emit(path + '/stats', 'json', True,
                  json.dumps({'dev': dev,
                              'root_dev': root_dev,
                              'used': used,
                              'avail': avail,
                              'is_ssd': is_ssd}))
        return dev

    def collect_osd(self, path, host, osd_id):
        path = "{0}/osd/{1}/".format(path, osd_id)
        osd_cfg_cmd = "sudo ceph -f json --admin-daemon /var/run/ceph/ceph-osd.{0}.asok config show"
        ok, data = check_output_ssh(host, self.opts, osd_cfg_cmd.format(osd_id))
        self.emit(path + "config", 'json', ok, data)
        assert ok

        osd_cfg = json.loads(data)
        self.emit_device_info(host, path + "journal", str(osd_cfg['osd_journal']))
        self.emit_device_info(host, path + "data", str(osd_cfg['osd_data']))
        self.ssh2emit(host, path + "osd_daemons", 'txt', "ps aux | grep ceph-osd")

    def collect_monitor(self, path, host, name):
        path = "{0}/mon/{1}/".format(path, host)
        # osd_cfg_cmd = "sudo ceph -f json --admin-daemon /var/run/ceph/ceph-osd.{0}.asok config show"
        # ok, data = self.ssh2emit(host, path + "config", 'json', osd_cfg_cmd.format(osd_id))
        self.ssh2emit(host, path + "mon_daemons", 'txt', "ps aux | grep ceph-mon")


class NodeCollector(Collector):

    name = 'node'
    run_alone = False

    node_commands = [
        ("lshw",      "xml", "lshw -xml"),
        ("lsblk",     "txt", "lsblk -a"),
        ("diskstats", "txt", "cat /proc/diskstats"),
        ("uname",     "txt", "uname -a"),
        ("dmidecode", "txt", "dmidecode"),
        ("meminfo",   "txt", "cat /proc/meminfo"),
        ("loadavg",   "txt", "cat /proc/loadavg"),
        ("cpuinfo",   "txt", "cat /proc/cpuinfo"),
        ("mount",     "txt", "mount"),
        ("ipa",       "txt", "ip a")
    ]

    def collect_node(self, path, host):
        path = 'hosts/' + host + '/'
        for path_off, frmt, cmd in self.node_commands:
            self.ssh2emit(host, path + path_off, frmt, cmd)


class PerfCollector(Collector):
    name = 'performance'
    run_alone = True

    def collect_node(self, path, host):
        path = 'hosts/' + host + '/'
        self.ssh2emit(host, path + "vmstat", "txt",
                      "vmstat 1 {0}".format(self.opts.stat_collect_seconds))
        self.ssh2emit(host, path + "iostat", "txt",
                      "iostat -x 1 {0}".format(self.opts.stat_collect_seconds))
        self.ssh2emit(host, path + "top", "txt",
                      "top -b -d {0} -n 10".format(self.opts.stat_collect_seconds))


class CephDiscovery(object):
    def __init__(self, opts):
        self.opts = opts
        self.ceph_cmd = "ceph -c {0.conf} -k {0.key} --format json ".format(self.opts)

    def discover(self):
        ok, res = check_output(self.ceph_cmd + "mon_status")
        assert ok
        for node in json.loads(res)['monmap']['mons']:
            yield 'monitor', str(node['name']), {'name': node['name']}

        ok, res = check_output(self.ceph_cmd + "osd tree")
        assert ok
        for node in json.loads(res)['nodes']:
            if node['type'] == 'host':
                for osd_id in node['children']:
                    yield 'osd', str(node['name']), {'osd_id': osd_id}


def save_results_th_func(opts, res_q, out_folder):
    try:
        while True:
            val = res_q.get()
            if val is None:
                break

            ok, path, frmt, out = val

            while '//' in path:
                path.replace('//', '/')

            while path.startswith('/'):
                path = path[1:]

            while path.endswith('/'):
                path = path[:-1]

            fname = os.path.join(out_folder, path + '.' + frmt)
            dr = os.path.dirname(fname)

            if not os.path.exists(dr):
                os.makedirs(dr)

            if frmt == 'bin':
                open(fname, "wb").write(out)
            elif frmt == json:
                if not opts.no_pretty_json:
                    out = json.dumps(json.loads(out), indent=4, sort_keys=True)
                open(fname, "wb").write(out)
            else:
                open(fname, "w").write(out)
    except:
        logger.exception("In save_results_th_func thread")


def discover_nodes(opts):
    discovers = [
        CephDiscovery
    ]

    nodes = collections.defaultdict(lambda: {})
    for discover_cls in discovers:
        discover = discover_cls(opts)
        for role, node, args in discover.discover():
            assert node not in nodes[role], "Duplicating node params"
            nodes[role][node] = args
            nodes['node'][node] = {}
    return nodes


def run_all(opts, run_q):
    def pool_thread():
        val = run_q.get()
        while val is not None:
            try:
                func, path, node, kwargs = val
                func(path, node, **kwargs)
            except:
                logger.exception("In worker thread")
            val = run_q.get()

    running_threads = []
    for i in range(opts.pool_size):
        th = threading.Thread(target=pool_thread)
        th.daemon = True
        th.start()
        running_threads.append(th)
        run_q.put(None)

    while True:
        time.sleep(0.01)
        if all(not th.is_alive() for th in running_threads):
            break


def setup_loggers(default_level=logging.INFO, log_fname=None):
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setLevel(default_level)

    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    colored_formatter = logging.Formatter(log_format, datefmt="%H:%M:%S")

    sh.setFormatter(colored_formatter)
    logger.addHandler(sh)

    if log_fname is not None:
        fh = logging.FileHandler(log_fname)
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(log_format, datefmt="%H:%M:%S")
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        logger.addHandler(fh)


def parse_args(argv):
    p = argparse.ArgumentParser()
    p.add_argument("-c", "--conf",
                   default="/etc/ceph/ceph.conf",
                   help="Ceph cluster config file")

    p.add_argument("-k", "--key",
                   default="/etc/ceph/ceph.client.admin.keyring",
                   help="Ceph cluster key file")

    p.add_argument("-l", "--log-level",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                   default="INFO",
                   help="Colsole log level")

    p.add_argument("-p", "--pool-size",
                   default=64, type=int,
                   help="Worker pool size")

    p.add_argument("-s", "--stat-collect-seconds",
                   default=15, type=int, metavar="SEC",
                   help="Collect stats from node for SEC seconds")

    p.add_argument("-d", "--disable", default=[],
                   nargs='*', help="Disable collect pattern")

    p.add_argument("-r", "--result", default=None, help="Result file")

    p.add_argument("-f", "--keep-folder", default=False,
                   action="store_true",
                   help="Keep unpacked data")

    p.add_argument("-j", "--no-pretty-json", default=False,
                   action="store_true",
                   help="Don't prettify json data")

    return p.parse_args(argv[1:])


logger_ready = False


def main(argv):
    if not check_output('which ceph')[0]:
        logger.error("No 'ceph' command available. Run this script from node, which has ceph access")
        return

    # TODO: Logs from down OSD
    opts = parse_args(argv)
    res_q = Queue.Queue()
    run_q = Queue.Queue()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out_folder = os.tempnam()

    os.makedirs(out_folder)

    setup_loggers(getattr(logging, opts.log_level),
                  os.path.join(out_folder, "log.txt"))
    global logger_ready
    logger_ready = True

    collector_settings = CollectSettings()
    map(collector_settings.disable, opts.disable)

    collectors = [
        CephDataCollector(opts, collector_settings, res_q),
        NodeCollector(opts, collector_settings, res_q),
        # PerfCollector(opts, collector_settings, res_q)
    ]

    nodes = discover_nodes(opts)
    nodes['master'][None] = {}

    for role, nodes_with_args in nodes.items():
        if role == 'node' or role == 'master':
            continue
        logger.info("Found %s hosts with role %s", len(nodes_with_args), role)

    logger.info("Found %s hosts total", len(nodes['node']))

    for role, nodes_with_args in nodes.items():
        for collector in collectors:
            if hasattr(collector, 'collect_' + role):
                coll_func = getattr(collector, 'collect_' + role)
                for node, kwargs in nodes_with_args.items():
                    run_q.put((coll_func, "", node, kwargs))

    save_results_thread = threading.Thread(target=save_results_th_func,
                                           args=(opts, res_q, out_folder))
    save_results_thread.daemon = True
    save_results_thread.start()

    run_all(opts, run_q)

    res_q.put(None)
    save_results_thread.join()

    if opts.result is None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            out_file = os.tempnam() + ".tar.gz"
    else:
        out_file = opts.result

    check_output("cd {0} ; tar -zcvf {1} *".format(out_folder, out_file))
    logger.info("Result saved into %r", out_file)

    if opts.keep_folder:
        shutil.rmtree(out_folder)
    else:
        logger.info("Temporary folder %r", out_folder)

if __name__ == "__main__":
    try:
        exit(main(sys.argv))
    except:
        if logger_ready:
            logger.exception("During main")
        else:
            raise
