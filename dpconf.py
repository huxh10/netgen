#! /usr/bin/python

import argparse
import json
import networkx as nx
from collections import defaultdict
from sets import Set
from random import randint
from net import Network


class DPConf(Network):
    def __init__(self, name):
        super(DPConf, self).__init__(name)
        # sw -> host1 -> host2 -> (in_link_obj, out_link_obj)
        self.sw_flow_tables = defaultdict(
                lambda: defaultdict(lambda: defaultdict(lambda: None)))
        self.host2ip = {}      # host_name -> ip

    def gen_shortest_path(self):
        self.sw_flow_tables = defaultdict(
                lambda: defaultdict(lambda: defaultdict(lambda: None)))
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
                    self.sw_flow_tables[b][host1.name][host2.name] = (in_link, out_link)

    def assign_host_addr(self):
        ips = Set()
        while len(ips) < self.host_num:
            ips.add(randint(0, 1 << 32 - 1))
        idx = 0
        for ip in ips:
            self.host2ip[self.topo['hosts'][idx].name] = ip
            #self.topo['host'][idx].ips.append(ip)
            idx += 1
        assert idx == self.host_num

    def _host2match(self, host1, host2):
        # 8 * 16 bits, big endian
        host1 = self.host2ip[host1]
        host2 = self.host2ip[host2]
        match = ''
        match += '{0:08b}'.format(host1 >> 24)
        match += ','
        match += '{0:08b}'.format((host1 >> 16) % (1 << 8))
        match += ','
        match += '{0:08b}'.format((host1 >> 8) % (1 << 8))
        match += ','
        match += '{0:08b}'.format(host1 % (1 << 8))
        match += ','
        match += '{0:08b}'.format(host2 >> 24)
        match += ','
        match += '{0:08b}'.format((host2 >> 16) % (1 << 8))
        match += ','
        match += '{0:08b}'.format((host2 >> 8) % (1 << 8))
        match += ','
        match += '{0:08b}'.format(host2 % (1 << 8))
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
        match += ','
        match += 'xxxxxxxx'
        return match

    def dump_conf(self):
        # topology
        file_name = 'output/topology.json'
        topo = {'topology': []}
        for l in self.topo['links']:
            topo['topology'].append({'src': l.intf1, 'dst': l.intf2})
            topo['topology'].append({'src': l.intf2, 'dst': l.intf1})
        with open(file_name, 'w') as out_file:
            json.dump(topo, out_file, indent=2)

        # router rules
        for i in xrange(0, self.sw_num):
            sw = self.topo['switches'][i]
            file_name = 'output/router' + str(sw.nid) + '.rules.json'
            router_conf = {'rule':[], 'ports': sw.intfs, 'id': sw.nid}
            for host1 in self.sw_flow_tables[sw.name]:
                for host2 in self.sw_flow_tables[sw.name][host1]:
                    (in_link, out_link) = self.sw_flow_tables[sw.name][host1][host2]
                    in_port = in_link.intf1 if in_link.sw1.name is sw.name else in_link.intf2
                    out_port = out_link.intf1 if out_link.sw1.name is sw.name else out_link.intf2
                    match = self._host2match(host1, host2)
                    router_conf['rule'].append({'rewrite': None, 'out_ports': [out_port], 'mask': None, 'in_ports': [in_port], 'action': 'fwd', 'match': match})
            with open(file_name, 'w') as out_file:
                json.dump(router_conf, out_file, indent=2)

        print 'configurations generated: %d switches %d hosts' % (self.sw_num, self.host_num)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', type=int, help='specify generator mode, 0: generate conf internally, 1: load from file')
    parser.add_argument('--topo', type=str, help='describe the topology type or topology file, e.g. a k-ary FatTree is FatTree-8, FatTree-128, FatTree-k')
    args = parser.parse_args()

    if args.mode is 0:
        if not args.topo:
            print 'please input the topology type'
            exit(0)
        dp_conf = DPConf(args.topo)
        print 'generate topology..'
        dp_conf.gen_ft_topo(int(args.topo.split('-')[1]))
        dp_conf.assign_host_addr()
        print 'calculate shortest path..'
        dp_conf.gen_shortest_path()
        print 'write configurations to file..'
        dp_conf.dump_conf()
        print 'Done'

    elif args.mode is 1:
        print 'to be constructed'