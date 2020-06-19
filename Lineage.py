#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: MingJia
# @Date:   2020-06-17 9:26:36
# @Last Modified by:   MingJia
# @Last Modified time: 2020-06-17 9:26:36
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class Node(object):
    """
    Get taxon lineage
    """
    def __init__(self,file_nodes):
        super(Node, self).__init__()
        self.parse_node(file_nodes)
        self.lineage = set()


    def parse_node(self,file_nodes):
        """
        Parse the file nodes.dmp

        :return:
        """
        logging.info("Parse the nodes.dmp...")
        self.children = {}
        with open(file_nodes, 'r') as IN:
            for line in IN:
                arr = [i.strip() for i in line.split('|')]
                if arr[0] == '1':
                    pass
                else:
                    self.children.setdefault(arr[1],set())
                    self.children[arr[1]].add(arr[0])

    def get_lineage(self,taxonid):
        """

        :return:
        """
        self.lineage.add(taxonid)
        if taxonid not in self.children:
            pass
        else:
            for i in self.children[taxonid]:
                self.get_lineage(i)




