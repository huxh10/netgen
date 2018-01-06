#! /usr/bin/python

from collections import defaultdict
import networkx as nx


MAX_PORT_NUM = 10000000     # support 10M-node scale net

'''
format
    host: 'h-<h_id>'
    switch: 's-<s_id>'
    interface: <s_id> * MAX_PORT_NUM + <port_id>
'''


class Link(object):
    def __init__(self, sw1, sw2, intf1=None, intf2=None):
        self.sw1 = sw1
        self.sw2 = sw2
        self.intf1 = sw1.new_port(intf1)
        self.intf2 = sw2.new_port(intf2)


class Node(object):
    def __init__(self, name):
        self.nid = int(name.split('-')[1])
        self.name = name
        self.next_port_id = 1
        self.intf_ids = []
        self.intf_name2id = {}
        self.ips = []

    def new_port(self, name=None):
        intf_id = self.nid * MAX_PORT_NUM + self.next_port_id
        self.intf_ids.append(intf_id)
        if name:
            self.intf_name2id[name] = intf_id
        self.next_port_id += 1
        return intf_id


class Network(object):
    '''
    basic network topology description
    '''
    def __init__(self, name):
        self.name = name            # net type name, e.g. FatTree-8, AS1032
        self.graph = nx.Graph()

        # topo['hosts'] -> [host_objs], topo['switches'] -> [sw_objs]
        # topo['links'] -> [link_objs]
        self.topo = {'hosts': [], 'switches': [], 'links': []}
        self.sw2node = defaultdict(lambda: None)
        self.host_num = 0
        self.sw_num = 0

        # node1 -> node2 -> link_obj
        self.nodes2link = defaultdict(lambda: defaultdict(lambda: None))

    def get_sw_by_name(self, name):
        return self.sw2node[name]

    def add_sw_by_name(self, name):
        self.sw2node[name] = Node(name)
        self.graph.add_node(name)
        self.topo['switches'].append(self.sw2node[name])

    def add_link_by_name(self, sw1, sw2, intf1=None, intf2=None):
        l = Link(self.get_sw_by_name(sw1), self.get_sw_by_name(sw2), intf1, intf2)
        self.graph.add_edge(sw1, sw2, weight = 1)
        self.topo['links'].append(l)
        self.nodes2link[sw1][sw2] = l
        self.nodes2link[sw2][sw1] = l

    def load_topo_fmt_w(self, topo_file):
        with open(topo_file, 'r') as in_file:
            l = in_file.readline()
            for l in in_file.readlines():
                intf1, intf2 = l[:-1].split(' ')
                sw1, port1 = intf1.split('-')
                sw2, port2 = intf2.split('-')
                if not self.get_node_by_name(sw1):
                    self.add_sw_by_name(sw1)
                if not self.get_node_by_name(sw2):
                    self.add_sw_by_name(sw2)
                if not self.nodes2link[sw1][sw2]:
                    self.add_link_by_name(sw1, sw2, intf1, intf2)

    def gen_ft_topo(self, pod):
        # pod: a cluster of edge and aggregation switches
        core_num = (pod / 2) ** 2
        aggr_num = pod * pod / 2
        edge_num = pod * pod / 2
        self.sw_num = core_num + aggr_num + edge_num
        self.host_num = ((pod / 2) ** 2) * pod
        self.link_num = 3 * pod * pod * pod / 4
        core_list = []
        aggr_list = []
        edge_list = []
        self.topo['hosts'] = []
        self.topo['switches'] = []
        self.topo['links'] = []
        self.nodes2link = defaultdict(lambda: defaultdict(lambda: None))

        # create switches
        for i in xrange(0, self.sw_num):
            sw_name = 's-' + str(i+1)
            sw = Node(sw_name)
            self.topo['switches'].append(sw)
            self.graph.add_node(sw_name)
            if i < core_num:
                core_list.append(sw)
            elif i < core_num + aggr_num:
                aggr_list.append(sw)
            else:
                edge_list.append(sw)

        # create hosts
        for i in xrange(0, self.host_num):
            host_name = 'h-' + str(i+1)
            host = Node(host_name)
            self.topo['hosts'].append(host)
            self.graph.add_node(host_name)

        # create links
        # core <--> aggregation
        index = 0
        for aggr in aggr_list:
            for i in xrange(0, pod / 2):
                l = Link(aggr, core_list[index])
                self.nodes2link[aggr.name][core_list[index].name] = l
                self.nodes2link[core_list[index].name][aggr.name] = l
                self.topo['links'].append(l)
                self.graph.add_edge(aggr.name, core_list[index].name, weight = 1)
                index = (index + 1) % core_num

        # aggregation <--> edge
        for i in xrange(0, aggr_num, pod /2):
            for j in xrange(0, pod / 2):
                for k in xrange(0, pod / 2):
                    l = Link(aggr_list[i + j], edge_list[i + k])
                    self.nodes2link[aggr_list[i + j].name][edge_list[i + k].name] = l
                    self.nodes2link[edge_list[i + k].name][aggr_list[i + j].name] = l
                    self.topo['links'].append(l)
                    self.graph.add_edge(aggr_list[i + j].name, edge_list[i + k].name, weight = 1)

        # edge <--> host
        index = 0
        for edge in edge_list:
            for i in xrange(0, pod / 2):
                l = Link(edge, self.topo['hosts'][index])
                self.nodes2link[edge.name][self.topo['hosts'][index].name] = l
                self.nodes2link[self.topo['hosts'][index].name][edge.name] = l
                self.topo['links'].append(l)
                self.graph.add_edge(edge.name, self.topo['hosts'][index].name, weight=1)
                index += 1
