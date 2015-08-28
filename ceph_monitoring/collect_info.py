import sys
import time
import json
import Queue
import shutil
import os.path
import argparse
import warnings
import threading
import functools
import subprocess


def check_output(cmd):
    p = subprocess.Popen(cmd, shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    out = p.communicate()
    code = p.wait()
    if code != 0:
        raise subprocess.CalledProcessError(code, cmd)
    return out[0]


class CephCluster(object):
    def __init__(self, opts):
        self.ceph_cmd = "ceph -c {0.conf} -k {0.key}".format(opts)
        self.opts = opts

    def jrun(self, cmd):
        return json.loads(self.run(cmd))

    def run(self, cmd):
        return check_output(self.ceph_cmd +
                            " " + cmd +
                            " --format json")

    def get_mon_hosts(self):
        return [str(node['name'])
                for node in self.jrun("mon_status")['monmap']['mons']]

    def get_osd_hosts(self):
        for node in self.jrun("osd tree")['nodes']:
            if node['type'] == 'host':
                for osd_id in node['children']:
                    yield str(node['name']), osd_id


def collect_master_data(ceph, opts):
    mdir = os.path.join(opts.out_folder, "master")
    if not os.path.isdir(mdir):
        os.makedirs(mdir)

    for cmd in ['osd tree', 'pg dump', 'df', 'auth list',
                'health', 'health detail', "mon_status"]:
        fname = os.path.join(mdir, cmd.replace(" ", "_") + ".json")
        open(fname, "w").write(ceph.run(cmd))

    lspools = ceph.run("osd lspools")
    open(os.path.join(mdir, "osd_lspools.json"), "w").write(lspools)

    pool_data = {}
    for pool in json.loads(lspools):
        pool_name = pool['poolname']
        pool_data[pool_name] = {}
        for stat in ['size', 'min_size', 'crush_ruleset']:
            val = ceph.run("osd pool get {0} {1}".format(pool_name, stat))
            pool_data[pool_name][stat] = json.loads(val)[stat]

    open(os.path.join(mdir, "pool_stats.json"), "w").write(json.dumps(pool_data))

    open(os.path.join(mdir, "s.json"), "w").write(ceph.run("-s"))
    fname = os.path.join(mdir, "crushmap.bin")
    ceph.run("osd getcrushmap -o " + fname)


def check_output_ssh(host, opts, cmd):
    ssh_opts = "-o LogLevel=quiet -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    return check_output("ssh {2} {0} {1}".format(host, cmd, ssh_opts))


def collect_osd_data(ceph, opts, osd_id, host):
    mdir = os.path.join(opts.out_folder, "osd-{0}".format(osd_id))
    if not os.path.isdir(mdir):
        os.makedirs(mdir)

    mjoin = functools.partial(os.path.join, mdir)

    osd_cfg_cmd = "sudo ceph --admin-daemon /var/run/ceph/ceph-osd.{0}.asok config show --format json"
    osd_cfg = json.loads(check_output_ssh(host, opts, osd_cfg_cmd.format(osd_id)))
    journal = str(osd_cfg['osd_journal'])
    data = str(osd_cfg['osd_data'])

    def get_device(fname):
        dev_str = check_output_ssh(host, opts, "df " + fname)

        dev_str = dev_str.strip()
        dev = dev_str.strip().split("\n")[1].split()[0]
        used = int(dev_str.strip().split("\n")[1].split()[2]) * 1024
        avail = int(dev_str.strip().split("\n")[1].split()[3]) * 1024

        if dev == 'udev':
            dev = fname

        dev_info = check_output_ssh(host, opts, "sudo hdparm -I " + dev)
        return {'dev': dev, 'used': used, 'avail': avail}, dev_info

    journal_dev_info, j_hdparm = get_device(journal)
    data_dev_info, d_hdparm = get_device(data)

    config = {
        'journal_dev': journal_dev_info,
        'data_dev': data_dev_info
    }

    open(mjoin("data_dev.txt"), "w").write(d_hdparm)
    open(mjoin("journal_dev.txt"), "w").write(j_hdparm)
    open(mjoin("config.json"), "w").write(json.dumps(config))


def collect_mon_data(ceph, opts, host):
    pass


def parse_args(argv):
    p = argparse.ArgumentParser()
    p.add_argument("-c", "--conf",
                   default="/etc/ceph/ceph.conf",
                   help="Ceph cluster config file")

    p.add_argument("-k", "--key",
                   default="/etc/ceph/ceph.client.admin.keyring",
                   help="Ceph cluster key file")

    p.add_argument("-p", "--pool-size",
                   default=32, type=int,
                   help="Worker pool size")

    p.add_argument("-o", "--out-folder", default=None, help="Result folder")

    return p.parse_args(argv[1:])


def pool_thread(q):
    val = q.get()
    while val is not None:
        val[0](*val[1:])
        val = q.get()


def main(argv):
    # TODO: Logs from down OSD
    opts = parse_args(argv)
    ceph = CephCluster(opts)

    if opts.out_folder is None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            opts.out_folder = os.tempnam()
            print "Results would be stored into", opts.out_folder

    q = Queue.Queue()
    q.put((collect_master_data, ceph, opts))

    for host, osd_id in ceph.get_osd_hosts():
        q.put((collect_osd_data, ceph, opts, osd_id, host))

    for host in ceph.get_mon_hosts():
        q.put((collect_mon_data, ceph, opts, host))

    running_threads = []
    for i in range(opts.pool_size):
        th = threading.Thread(target=pool_thread, args=[q])
        th.daemon = True
        th.start()
        running_threads.append(th)
        q.put(None)

    while True:
        time.sleep(0.01)
        if all(not th.is_alive() for th in running_threads):
            break

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        archive_fname = os.tempnam()

    check_output("cd {0} ; tar -zcvf {1} *".format(opts.out_folder, archive_fname))

    dest_fname = os.path.join(opts.out_folder, "compressed.tar.gz")
    shutil.move(archive_fname, dest_fname)
    print "Archive stored into ", dest_fname

if __name__ == "__main__":
    exit(main(sys.argv))
