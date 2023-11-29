# Start of server.py

# Should we just use HTTP instead of ZeroMQ?


import hashlib
import json
import math
import time
from bisect import bisect
from uuid import uuid4
import matplotlib.pyplot as plt

import zmq
import threading

N = 4

class HashTable:
    def __init__(self, nodes, replica_count=3):
        self._context = zmq.Context()
        self.sockets = {}
        self.threads = {}
        self.nodes = nodes
        self.read_quorum = len(nodes) // 2 + 1
        self.write_quorum = len(nodes) // 2 + 1


        self.ring = {}
        self.replica_count = replica_count

        self.sorted_keys = []
        router_address = "tcp://*:5554"
        self.router_socket = self._context.socket(zmq.ROUTER)
        self.router_socket.bind(router_address)

        router_address = router_address.replace("*", "localhost")

        for node in nodes:
            socket = self._context.socket(zmq.DEALER)
            # socket.bind("tcp://*:" + node.split(":")[2])
            socket.setsockopt(zmq.IDENTITY, node.encode('utf-8'))
            socket.connect(router_address)

            thread = threading.Thread(target=self.listen, args=(socket,))
            thread.start()

            self.sockets[node] = socket
            self.threads[node] = thread

        self.generate_ring()

    def get_node(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned.
        If the hash ring is empty, `None` is returned.
        """
        pos = self.get_node_pos(string_key)
        if pos is None:
            return None
        return self.ring[self.sorted_keys[pos]]

    def get_node_key(self, node, replica_index):
        return hashlib.sha256(f"{node}-{replica_index}".encode()).hexdigest()

    def get_node_pos(self, key):
        if not self.ring:
            return None

        hash_key = self.hash_key(key)
        for i, ring_key in enumerate(self.sorted_keys):
            if hash_key <= ring_key:
                return i

        # If the hash is greater than all keys, loop back to the first node
        return 0

    def get_replica_nodes(self, primary_node):
        if not self.ring:
            return []

        primary_index = self.sorted_keys.index(self.get_node_key(primary_node, 0))
        #skip the primary node
        i = 0
        j = 0
        replica_indices = []
        index = primary_index - 1
        while j < len(self.sorted_keys) and i < N:
            # get the node at the next index, clockwise
            next_node = self.ring[self.sorted_keys[index]]
            if next_node != primary_node:
                replica_indices.append(index)
                i += 1
            index = (index - 1) % len(self.sorted_keys)
            j += 1

        return [self.ring[self.sorted_keys[i]] for i in replica_indices]

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


    # TODO insert and delete node
    def generate_ring(self):
        """
        https://gist.github.com/soekul/a240f9e11d6439bd0237c4ab45dce7a2
        :return:
        """
        for node in self.nodes:
            for i in range(self.replica_count):
                key = self.get_node_key(node, i)
                self.ring[key] = node
                self.sorted_keys.append(key)


        self.sorted_keys.sort()

        print(self.sorted_keys)
        print(self.ring)


    def listen(self, socket: zmq.Socket):
        while True:
            # TODO usar polling ou selector para verificar/escolher se tem mensagens??

            request = socket.recv()
            print(request.decode('utf-8'), " received")

            key = json.loads(request)['key']
            # request = socket.recv( zmq.NOBLOCK)

            # print(request, " received")

            # Receive the response from the appropriate node using the ZeroMQ socket for that node.

            # Send the response to the client.

            socket.send(json.dumps({key: "bar"}).encode('utf-8'))

    def expose(self):
        # Start a thread to listen for requests on the client socket.

        thread = threading.Thread(target=self.listen_for_client_requests)
        thread.start()

    def listen_for_client_requests(self):
        while True:
            identity, message = self.router_socket.recv_multipart()
            if identity in self.sockets:
                continue

            request = message.decode('utf-8')
            json_request = json.loads(request)
            print(request, " received")

            # planning more cases, but only get for now
            # assume request is a json with key and request type
            if json_request['type'] == 'get':
                key = json_request['key']
                # Receive the response from the appropriate node using the ZeroMQ socket for that node.
                # Send the response to the client.

                value = self.get(key)
                print(value)
                self.router_socket.send_multipart([identity, value])

            elif json_request['type'] == 'put':
                shopping_list = json_request['shopping_list']
                # Receive the response from the appropriate node using the ZeroMQ socket for that node.
                # Send the response to the client.

                self.put(shopping_list)
                # list_id = shopping_list['id']

                # value = self.get(key)
                # print(value)
                # self.router_socket.send_multipart([identity, value])

            # self.router_socket.send(response)

    def route(self, list_id: str):
        return self.get_node(list_id)

    def get(self, key):
        # Create a request message.

        request = {
            "key": key
        }

        # request = json.dumps(data).encode()

        # Send the request to the appropriate node.

        node = self.route(key)
        # socket.recv_multipart()

        # socket.send_json(request)
        self.router_socket.send_multipart([node.encode('utf-8'), json.dumps(request).encode('utf-8')])

        # Receive the response from the appropriate node.
        # response = socket.recv()
        client_id, response = self.router_socket.recv_multipart()
        print(client_id)

        # time.sleep(1)
        return response

    def put(self, shopping_list: dict):
        list_id = shopping_list['id']
        #         this id is a uuid, use consisten hashing to find the node
        node = self.route(list_id)

        # TODO how to store each list???


class DynamoNode:
    def __init__(self, name):
        self.name = name
        self.data = {}

    def store_data(self, key, value):
        self.data[key] = value

    def get_data(self, key):
        return self.data.get(key)

    def replicate_data(self, key, value, successor_node):
        successor_node.store_data(key, value)

    def read_data_quorum(self, key, hash_ring, dynamo_nodes, quorum_size):
        primary_node = hash_ring.get_node(key)
        replicas = hash_ring.get_replica_nodes(primary_node)

        read_responses = []
        if self.name == primary_node:
            read_responses.append(self.data.get(key))

        for replica_node in replicas:
            replica_node = dynamo_nodes[replica_node]
            if replica_node != primary_node:
                replica_data = replica_node.get_data(key)
                if replica_data is not None:
                    read_responses.append(replica_data)

            if len(read_responses) >= quorum_size:
                break

        # validate size of read_responses after return??
        return read_responses

    def write_data_quorum(self, key, value, hash_ring, dynamo_nodes, quorum_size):
        primary_node = hash_ring.get_node(key)
        replicas = hash_ring.get_replica_nodes(primary_node)

        write_responses = []
        if self.name == primary_node:
            self.store_data(key, value)
            write_responses.append(True)

        for replica in replicas:
            replica_node = dynamo_nodes[replica]
            if replica_node != primary_node:
                replica_node.store_data(key, value)
                write_responses.append(True)
                # TODO message logic and confirmations of success

            # if len(write_responses) >= quorum_size:
            #     break

        return len([r for r in write_responses if r]) >= quorum_size


def store_data(key, value, hash_ring, dynamo_nodes):
    node = hash_ring.get_node(key)
    dynamo_node = dynamo_nodes[node]

    # Store data in the primary node
    dynamo_node.store_data(key, value)

    # Replicate data to the replica nodes
    replicas = hash_ring.get_replica_nodes(node) # set??
    for replica in replicas:
        replica_node = dynamo_nodes[replica]
        replica_node.store_data(key, value)


def read_data_quorum(key, quorum_size, hash_ring, dynamo_nodes):
    node = hash_ring.get_node(key)
    dynamo_node = dynamo_nodes[node]
    result = dynamo_node.read_data_quorum(key, hash_ring, dynamo_nodes, quorum_size)
    if len(result) < quorum_size:
        return None
    # todo validate if all results are the same
    return result

def write_data_quorum(key, value, quorum_size, hash_ring, dynamo_nodes):
    node = hash_ring.get_node(key)
    dynamo_node = dynamo_nodes[node]
    result = dynamo_node.write_data_quorum(key, value, hash_ring, dynamo_nodes, quorum_size)
    return result



if __name__ == "__main__":
    nodes = [
        "tcp://localhost:5555",
        "tcp://localhost:5556",
        "tcp://localhost:5557",
        "tcp://localhost:5558",
    ]



    hash_table = HashTable(nodes)
    dynamo_nodes = {node: DynamoNode(node) for node in nodes}
    R = 2
    W = 3
    # positions = []
    # # for 1000 random strings of length 10, plot the distribution of the positions of the got node in the ring
    # for i in range(10000):
    #     key = str(uuid4())
    #     positions.append(hash_table.get_node_pos(key))
    #     # print(get_node_pos(key))
    # l = len(hash_table.sorted_keys)
    # plt.hist(positions, bins=l)
    # plt.show()
    # print('Plotting done')

    # Example usage:
    store_data("key1", "value1", hash_table, dynamo_nodes)
    store_data("key2", "value2", hash_table, dynamo_nodes)
    store_data("key3", "value3", hash_table, dynamo_nodes)

    # Retrieve data from the appropriate node
    key_to_lookup = "key2"
    # node = hash_table.get_node(key_to_lookup)
    # result = dynamo_nodes[node].get_data(key_to_lookup)
    # print(f"Data for key '{key_to_lookup}': {result}")

    result = read_data_quorum(key_to_lookup, R, hash_table, dynamo_nodes)
    print(f"Data for key '{key_to_lookup}' with quorum: {result}")

    key_to_write = "key4"
    value_to_write = "value4"
    quorum_size = 2
    result = write_data_quorum(key_to_write, value_to_write, W, hash_table, dynamo_nodes)
    print(f"Write success: {result}")

    # Verify the data has been written
    read_result = dynamo_nodes[hash_table.get_node(key_to_write)].get_data(key_to_write)
    print(f"Data for key '{key_to_write}': {read_result}")

    # Start listening for requests on the client socket.
    hash_table.expose()

















