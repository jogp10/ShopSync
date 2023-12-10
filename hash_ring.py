import hashlib
import sys

from utils import REPLICA_COUNT, N


class HashRing:
    def __init__(self, nodes=[], replica_count=REPLICA_COUNT):
        self.ring = {}
        self.replica_count = replica_count
        self.sorted_keys = []
        self.nodes = set(nodes)
        self.generate_ring(self.nodes)

    def generate_ring(self, nodes):
        for node in nodes:
            for i in range(self.replica_count):
                key = self.get_node_key(node, i)
                self.ring[key] = node
                self.sorted_keys.append(key)

        self.sorted_keys.sort()

        # print(self.sorted_keys)
        # print(self.ring)

    def get_all_nodes(self):
        return list(self.nodes)

    def get_other_nodes(self, node):
        return [n for n in self.nodes if n != node]

    def get_node(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned.
        If the hash ring is empty, `None` is returned.
        """
        pos = self.get_node_pos(string_key)
        print(f"pos: {pos}")
        return self.ring[self.sorted_keys[pos]], pos

    def get_node_key(self, node, replica_index):
        return hashlib.sha256(f"{node}-{replica_index}".encode()).hexdigest()

    def get_node_pos(self, key):
        hash_key = self.hash_key(key)
        for i, ring_key in enumerate(self.sorted_keys):
            if hash_key <= ring_key:
                return i

        # If the hash is greater than all keys, loop back to the first node
        print(self.sorted_keys)
        return 0

    def get_replica_nodes(self, primary_node, primary_node_position, unhealthy_nodes=[]):
        if not self.ring:
            return []
        # skip the primary node
        i = 0  # number of nodes found
        j = 0  # number of nodes checked
        failed_nodes = []
        replica_indices = []
        index = (primary_node_position + 1) % len(self.sorted_keys)
        while j < len(self.sorted_keys) and i < N:
            # get the node at the next index, clockwise
            next_node = self.ring[self.sorted_keys[index]]
            if next_node != primary_node:
                if next_node not in unhealthy_nodes:
                    replica_indices.append(index)
                else:
                    failed_nodes.append(next_node)
                i += 1
            index = (index + 1) % len(self.sorted_keys)
            j += 1


        substitute_nodes = []
        if len(failed_nodes) > 0:
            print(f"Unhealthy count: {len(failed_nodes)}")
        #     start on the last replica index and gather the remaining nodes
            index = replica_indices[-1] if len(replica_indices) > 0 else (primary_node_position + 1) % len(self.sorted_keys)
            while i < N and j < len(self.sorted_keys):
                next_node = self.ring[self.sorted_keys[index]]
                if next_node != primary_node:
                    if next_node not in unhealthy_nodes:
                        replica_indices.append(index)
                        substitute_nodes.append(next_node)
                    i += 1
                index = (index + 1) % len(self.sorted_keys)
                j += 1

        return [self.ring[self.sorted_keys[i]] for i in replica_indices], failed_nodes, substitute_nodes

    def get_ideal_replica_nodes(self, primary_node, primary_node_position):
        """Do not consider failed nodes and add the primary node to the list"""
        if not self.ring:
            return []

        # skip the primary node
        i = 0
        j = 0
        replica_indices = []
        index = (primary_node_position + 1) % len(self.sorted_keys)
        while j < len(self.sorted_keys) and i < N:
            # get the node at the next index, clockwise
            next_node = self.ring[self.sorted_keys[index]]
            if next_node != primary_node:
                replica_indices.append(index)
                i += 1
            index = (index + 1) % len(self.sorted_keys)
            j += 1

        return [self.ring[self.sorted_keys[i]] for i in replica_indices] + [primary_node]
    def hash_key(self, key):
        return hashlib.sha256(key.encode()).hexdigest()

    def add_node(self, node):
        if node in self.nodes:
            return
        for i in range(self.replica_count):
            key = self.get_node_key(node, i)
            self.ring[key] = node
            self.sorted_keys.append(key)

        self.sorted_keys.sort()
        self.nodes.add(node)

    def remove_node(self, node):
        if node not in self.nodes:
            return
        for i in range(self.replica_count):
            key = self.get_node_key(node, i)
            del self.ring[key]
            self.sorted_keys.remove(key)
        self.nodes.remove(node)

    def print_ring(self):
        print("Hash Ring:")
        for key in self.sorted_keys:
            print(f"{key}: {self.ring[key]}")
