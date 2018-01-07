#! /usr/bin/python

import argparse
import json
import subprocess
import re
import os.path
import networkx as nx
from collections import defaultdict
from sets import Set
from random import randint
from net import Network


class FlowEntry(object):
    def __init__(self):
        self.inport = None              # integer id
        self.srcip_prefix = None        # integer
        self.srcip_mask = None          # integer 0-32
        self.dstip_prefix = None        # integer
        self.dstip_mask = None          # integer 0-32
        self.outport = None             # integer id

    def rule(self, intf_ids):
        if not self.outport:
            return None
        in_ports = [self.inport] if self.inport else [p for p in intf_ids if p != self.outport]
        ret = {'rewrite': None, 'out_ports': [self.outport], 'mask': None, 'in_ports': in_ports, 'action': 'fwd', 'match': self._match()}
        return ret

    def _ip(self, prefix, mask):
        ip = ''
        ip += '{0:08b}'.format(prefix >> 24)
        ip += ','
        ip += '{0:08b}'.format((prefix >> 16) % (1 << 8))
        ip += ','
        ip += '{0:08b}'.format((prefix >> 8) % (1 << 8))
        ip += ','
        ip += '{0:08b}'.format(prefix % (1 << 8))
        if mask <= 8:
            ip = ip[: mask]
            for i in range(mask, 8):
                ip += 'x'
            ip += ',xxxxxxxx,xxxxxxxx,xxxxxxxx,'
        elif mask >= 9 and mask <= 16:
            ip = ip[: mask + 1]
            for i in range(mask, 16):
                ip += 'x'
            ip += ',xxxxxxxx,xxxxxxxx,'
        elif mask >= 17 and mask <= 24:
            ip = ip[: mask + 2]
            for i in range(mask, 24):
                ip += 'x'
            ip += ',xxxxxxxx,'
        else:
            assert mask <= 32
            ip = ip[: mask + 3]
            for i in range(mask, 32):
                ip += 'x'
            ip += ','
        return ip

    def _match(self):
        # 8 * 16 bits, big endian
        match = ''
        if self.srcip_mask:
            match += self._ip(self.srcip_prefix, self.srcip_mask)
        else:
            match += 'xxxxxxxx,xxxxxxxx,xxxxxxxx,xxxxxxxx,'
        match += self._ip(self.dstip_prefix, self.dstip_mask)
        match += 'xxxxxxxx'
        match += ','
        match += 'xxxxxxxx'
        match += ','
        match += 'xxxxxxxx'
        match += ','
        match += 'xxxxxxxx'
        match += ','
        match += 'xxxxxxxx'
        match += ','
        match += 'xxxxxxxx'
        match += ','
        match += 'xxxxxxxx'
        match += ','
        match += 'xxxxxxxx'
        return match


class DPConf(Network):
    def __init__(self, name):
        super(DPConf, self).__init__(name)
        # sw -> host1 -> host2 -> (in_link_obj, out_link_obj)
        self.sw_flow_tables = defaultdict(
                lambda: defaultdict(lambda: defaultdict(lambda: None)))
        self.host2ip = defaultdict(lambda: [])      # host_name -> [ips]
        self.sw_fts = defaultdict(lambda: [])

    def assign_host_addr(self, host_ip_num):
        ips = Set()
        while len(ips) < self.host_num * host_ip_num:
            ips.add(randint(0, 1 << 32 - 1))
        idx = 0
        for ip in ips:
            self.host2ip[self.topo['hosts'][idx / host_ip_num].name].append(ip)
            #self.topo['host'][idx].ips.append(ip)
            idx += 1
        assert idx == self.host_num * host_ip_num

    def gen_shortest_path(self):
        self.sw_fts = defaultdict(lambda: [])
        current = 0
        for host1 in self.topo['hosts']:
            for host2 in self.topo['hosts']:
                if host1 is host2:
                    continue
                try:
                    paths = list(nx.all_shortest_paths(self.graph, host1.name, host2.name, 'weight'))
                except nx.exception.NetworkXNoPath:
                    continue
                path = paths[current % len(paths)]
                # spread hostpaths on all path candidates evenly
                current += 1
                path = zip(path, path[1:], path[2:])
                for (a, b, c) in path:
                    in_link = self.nodes2link[a][b]
                    out_link = self.nodes2link[b][c]
                    for host1_ip in self.host2ip[host1.name]:
                        for host2_ip in self.host2ip[host2.name]:
                            fe = FlowEntry()
                            fe.inport = in_link.intf1 if in_link.sw1.name is b else in_link.intf2
                            fe.outport = out_link.intf1 if out_link.sw1.name is b else out_link.intf2
                            fe.srcip_prefix = host1_ip
                            fe.srcip_mask = 32
                            fe.dstip_prefix = host2_ip
                            fe.dstip_mask = 32
                            self.sw_fts[b].append(fe)

    def load_router_fwd_w(self, if_dir):
        conf_dir = if_dir + '/'
        for sw in self.topo['switches']:
            route_file = conf_dir + sw.name + '.route.txt'
            if not os.path.isfile(route_file):
                continue
            with open(conf_dir + sw.name + '.route.txt', 'r+') as in_file:
                # skip header
                in_file.readline()
                in_file.readline()
                in_file.readline()
                in_file.readline()
                in_file.readline()
                # routes
                for l in in_file.readlines():
                    iterms = re.split(' +', l[:-1])
                    if not iterms[0]:
                        iterms = iterms[1:]
                    if 'In' in iterms[-1]:
                        continue
                    fe = FlowEntry()
                    prefix, mask = iterms[0].split('/')
                    fe.dstip_mask = int(mask)
                    p1, p2, p3, p4 = map(lambda x: int(x), prefix.split('.'))
                    fe.dstip_prefix = (p1 << 24) + (p2 << 16) + (p3 << 8) + p4
                    fe.outport = sw.intf_name2id[iterms[-1]]
                    if not fe.outport:
                        print 'Switch:%s Interface:%s is not in topology' % (sw.name, iterms[-1])
                        print sw.intf_name2id
                        exit(-1)
                    self.sw_fts[sw.name].append(fe)
            self.sw_fts[sw.name].sort(key=lambda x: x.dstip_mask)

    def dump_conf(self):
        conf_dir = self.name + '/'
        subprocess.call(['rm', '-rf', conf_dir])
        subprocess.call(['mkdir', conf_dir])
        # topology
        file_name = conf_dir + 'topology.json'
        topo = {'topology': []}
        for l in self.topo['links']:
            if 'h' not in l.sw1.name and 'h' not in l.sw2.name:
                topo['topology'].append({'src': l.intf1, 'dst': l.intf2})
                topo['topology'].append({'src': l.intf2, 'dst': l.intf1})
        with open(file_name, 'w') as out_file:
            json.dump(topo, out_file, indent=2)

        # router rules
        rule_num = 0
        for i in xrange(0, self.sw_num):
            sw = self.topo['switches'][i]
            file_name = conf_dir + 'router' + str(sw.nid) + '.rules.json'
            router_conf = {'rules': [], 'ports': sw.intf_ids, 'id': sw.nid}
            for fe in self.sw_fts[sw.name]:
                rule = fe.rule(sw.intf_ids)
                if rule:
                    router_conf['rules'].append(rule)
                rule_num += 1
            with open(file_name, 'w') as out_file:
                json.dump(router_conf, out_file, indent=2)

        print 'configurations generated: %d switches %d hosts %d links %d rules' % (self.sw_num, self.host_num, self.link_num, rule_num)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', type=int, help='specify generator mode, 0: generate conf internally, 1: load from file')
    parser.add_argument('--topo', type=str, help='describe the topology type or topology file, e.g. a k-ary FatTree is FatTree-8, FatTree-128, FatTree-k')
    parser.add_argument('--if_dir', type=str, help='specify the input directory', default='ifd')
    parser.add_argument('--of_dir', type=str, help='specify the output directory', default='ofd')
    parser.add_argument('--host_ip_num', type=int, help='the number of ips one host has, default is 1', default=1)
    args = parser.parse_args()

    if args.mode is 0:
        if not args.topo:
            print 'please input the topology type'
            exit(0)
        dp_conf = DPConf(args.topo)
        print 'generate topology..'
        dp_conf.gen_ft_topo(int(args.topo.split('-')[1]))
        dp_conf.assign_host_addr(args.host_ip_num)
        print 'calculate shortest path..'
        dp_conf.gen_shortest_path()
        print 'write configurations to file..'
        dp_conf.dump_conf()
        print 'Done'

    elif args.mode is 1:
        dp_conf = DPConf(args.of_dir)
        dp_conf.load_topo_fmt_w(args.topo)
        dp_conf.load_router_fwd_w(args.if_dir)
        dp_conf.dump_conf()
