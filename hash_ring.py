import hashlib
import uuid

import matplotlib.pyplot as plt


class HashRing:

    def __init__(self, nodes=None, replica_count=3):
        self.nodes = nodes or []
        self.replica_count = replica_count
        self.ring = {}
        self.sorted_keys = []

        self.generate_ring()

    def generate_ring(self):
        for node in self.nodes:
            for i in range(self.replica_count):
                key = self.get_node_key(node, i)
                self.ring[key] = node
                self.sorted_keys.append(key)

        self.sorted_keys.sort()

    def get_node_key(self, node, replica_index):
        return hashlib.sha256(f"{node}-{replica_index}".encode()).hexdigest()

    def get_node(self, key):
        if not self.ring:
            return None

        hash_key = self.hash_key(key)
        for ring_key in self.sorted_keys:
            if hash_key <= ring_key:
                return self.ring[ring_key]

        # If the hash is greater than all keys, loop back to the first node
        return self.ring[self.sorted_keys[0]]

    def get_node_pos(self, key):
        if not self.ring:
            return None

        hash_key = self.hash_key(key)
        for i, ring_key in enumerate(self.sorted_keys):
            if hash_key <= ring_key:
                return i

        # If the hash is greater than all keys, loop back to the first node
        return 0


    def hash_key(self, key):
        return hashlib.sha256(key.encode()).hexdigest()

    def add_node(self, node):
        for i in range(self.replica_count):
            key = self.get_node_key(node, i)
            self.ring[key] = node
            self.sorted_keys.append(key)

        self.sorted_keys.sort()

    def remove_node(self, node):
        for i in range(self.replica_count):
            key = self.get_node_key(node, i)
            del self.ring[key]
            self.sorted_keys.remove(key)

    def print_ring(self):
        print("Hash Ring:")
        for key in self.sorted_keys:
            print(f"{key}: {self.ring[key]}")


if __name__ == "__main__":
    # Example usage:
    nodes = ["node1", "node2", "node3"]
    hash_ring = HashRing(nodes=nodes, replica_count=3)
    hash_ring.print_ring()

    # Add a new node to the ring
    new_node = "node4"
    hash_ring.add_node(new_node)
    print(f"\nAfter adding {new_node}:")
    hash_ring.print_ring()

    # Remove a node from the ring
    removed_node = "node2"
    hash_ring.remove_node(removed_node)
    print(f"\nAfter removing {removed_node}:")
    hash_ring.print_ring()

    # Get node for a key
    key_to_lookup = "example_key"
    node_for_key = hash_ring.get_node(key_to_lookup)
    print(f"\nNode for key '{key_to_lookup}': {node_for_key}")


    # create a new hash ring with 8 nodes
    nodes = [f"node{i}" for i in range(8)]
    hash_ring2 = HashRing(nodes=nodes, replica_count=24)

    positions = []
    for i in range(10000):
        key = uuid.uuid4().hex
        pos = hash_ring2.get_node_pos(key)
        positions.append(pos)

    plt.hist(positions, bins=len(hash_ring2.sorted_keys))
    plt.show()
    # print the position that appears the most
    print(max(set(positions), key=positions.count))
    print('Plotting done')