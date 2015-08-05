import sys
import copy
import os.path
import collections

import texttable

from ceph_report_templ import templ


class DevLoad(object):
    def __init__(self, name):
        self.name = name
        self.values = collections.defaultdict(lambda: [])
        self.is_partition = name[-1].isdigit()

    def get_dev(self):
        name = self.name
        while name[-1].isdigit():
            name = name[:-1]
        return name

    def get_avg_sz(self):
        return self.sum_data() / self.sum_iops()

    def sum_data(self):
        return sum(map(float, self.values['wkB/s']))

    def sum_iops(self):
        return sum(map(float, self.values['w/s']))


def load_io_usage(fname):
    fc = [i.strip()
          for i in open(fname).read().split("\n")
          if i.strip() != ""]

    devs = fc[0].split()
    num_devs = len(fc[1].split())
    fc = fc[2:]
    pairs = zip(devs[::2], devs[1::2])
    per_dev = {}
    names = ('rrqm/s wrqm/s r/s w/s' +
             ' rkB/s wkB/s avgrq-sz avgqu-sz' +
             ' await r_await w_awaits vctm util').split()

    for offset in range(len(fc) / num_devs):
        data = fc[offset * num_devs:(offset + 1) * num_devs]
        for line in data:
            items = line.split()
            if items[0] not in per_dev:
                obj = DevLoad(items[0])
                per_dev[items[0]] = obj
            else:
                obj = per_dev[items[0]]

            for metr_name, val in zip(names, map(float, items[1:])):
                obj.values[metr_name].append(val)
    return pairs, per_dev


class Host(object):
    def __init__(self, name, files):
        self.name = name
        self.files = files
        self.io_usage = None
        self.devs_pairs = None
        self.pure_dev_names = None

    def __str__(self):
        return "Host({0.name}, {0.files})".format(self)

    def __repr__(self):
        return str(self)

    def agg_partitions(self):
        self.pure_dev_names = set()
        devs = collections.defaultdict(lambda: [])
        for part_name, part_obj in self.io_usage.items():
            if not part_obj.is_partition:
                self.pure_dev_names.add(part_name)
            else:
                devs[part_obj.get_dev()].append(part_name)

        for dev, parts in devs.items():
            if len(parts) == 1:
                dev_obj = copy.deepcopy(self.io_usage[parts[0]])
                dev_obj.name = dev
            else:
                dev_obj = DevLoad(dev)
                part_objs = map(self.io_usage.get, parts)

                utils = [part_obj.values['util']
                         for part_obj in part_objs]
                # this is actually wrong
                dev_obj.values['util'] = [min(i, 100)
                                          for i in map(sum, zip(*utils))]

                for name in ('w/s', 'r/s', 'rkB/s', 'wkB/s'):
                    vals = [part_obj.values[name]
                            for part_obj in part_objs]
                    dev_obj.values[name] = map(sum, zip(*vals))

            self.io_usage[dev] = dev_obj
            self.pure_dev_names.add(dev)


hosts = {}

for fname in sys.argv[1:]:
    base_fname = os.path.basename(fname)
    assert base_fname.startswith('osd_mon')
    pref, tp = base_fname.rsplit('.', 1)
    _, name = pref.split('.', 1)

    if name in hosts:
        hosts[name].files[tp] = fname
    else:
        hosts[name] = Host(name, {tp: fname})


all_devs = []
data_param = []

for host in hosts.values():
    host.devs_pairs, host.io_usage = load_io_usage(host.files['io'])
    host.agg_partitions()

tab = texttable.Texttable(max_width=120)
tab.set_deco(tab.HEADER | tab.VLINES | tab.BORDER)
tab.set_cols_align(['r', 'r', 'r', 'r', 'r'])
tab.header(['host::dev', 'avg_sz', 'wr Mb', 'wr kIO', 'avg util %'])

hosts_list = sorted(hosts.values(), key=lambda x: x.name)
for host in hosts_list:
    for dev_name in sorted(host.pure_dev_names):
        dev = host.io_usage[dev_name]
        tab.add_row([
            host.name + "::" + dev_name,
            int(dev.get_avg_sz()),
            int(dev.sum_data() / 1024),
            int(dev.sum_iops() / 1000),
            int(sum(dev.values['util']) / len(dev.values['util']))])
    if host != hosts_list[-1]:
        tab.add_row(['----'] * 5)

print tab.draw()
exit(1)

for host in hosts_list:
    for dev_name in sorted(host.pure_dev_names):
        dev = host.io_usage[dev_name]
        dname = "'{0}.{1}'".format(host.name, dev_name)
        all_devs.append(dname)
        vals = ','.join(map(str, dev.values['util']))
        data_param.append('{0}: [{1}]'.format(dname, vals))

print templ.replace('{{devs}}', ", ".join(all_devs)).replace('{{data}}', ",".join(data_param))
