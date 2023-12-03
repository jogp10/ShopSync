# Start of server.py

# Should we just use HTTP instead of ZeroMQ?
import hashlib
import json
import queue
import sys
import time
from uuid import uuid4
import matplotlib.pyplot as plt

import zmq
import threading
import asyncio

from crdt import ShoppingListCRDT
from node import DynamoNode
from utils import *

N = 4
R_QUORUM = 2
W_QUORUM = 3


class HashRing:
    def __init__(self, nodes, replica_count):
        self.ring = {}
        self.replica_count = replica_count
        self.sorted_keys = []
        self.generate_ring(nodes)

    def generate_ring(self, nodes):
        """
        https://gist.github.com/soekul/a240f9e11d6439bd0237c4ab45dce7a2
        :return:
        """
        for node in nodes:
            for i in range(self.replica_count):
                key = self.get_node_key(node, i)
                self.ring[key] = node
                self.sorted_keys.append(key)

        self.sorted_keys.sort()

        # print(self.sorted_keys)
        # print(self.ring)

    def get_node(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned.
        If the hash ring is empty, `None` is returned.
        """
        pos = self.get_node_pos(string_key)
        return self.ring[self.sorted_keys[pos]]

    def get_node_key(self, node, replica_index):
        return hashlib.sha256(f"{node}-{replica_index}".encode()).hexdigest()

    def get_node_pos(self, key):
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
        # skip the primary node
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


class Router:

    def __init__(self, nodes=[], replica_count=3):
        self._context = zmq.Context()
        self.sockets = {}
        self.threads = {}
        self.nodes = nodes
        self.hash_ring = HashRing(nodes, replica_count)
        self.tasks_queue = queue.Queue()
        self.read_quorum_requests_state = {}
        self.write_quorum_requests_state = {}

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

    def add_node(self, node_address):
        self.nodes.append(node_address)
        self.hash_ring.add_node(node_address)

    def listen(self, socket: zmq.Socket):
        identity = socket.IDENTITY.decode('utf-8')
        socket_node = DynamoNode(identity)
        # for DEBUG/DEV ONLY, TODO remove later
        socket_node.increment_item("key2", 9)

        # create new socket for message receiving
        # router_socket = self._context.socket(zmq.ROUTER)
        # router_socket.bind(identity)

        while True:
            # TODO usar polling ou selector para verificar/escolher se tem mensagens?

            #  receve from either the router or the client, nowaits
            request = socket.recv_json()

            key = request['key']

            # switch case on request type
            match get_request_type(request):

                case MessageType.GET:
                    response = {
                        "type": MessageType.GET_RESPONSE,
                        "key": key,
                        "value": socket_node.get_data(key),
                        "address": socket.IDENTITY.decode('utf-8'),
                        "quorum_id": request['quorum_id']
                    }
                    # time.sleep(1)
                    socket.send_json(response)
                    continue

                case MessageType.PUT:
                    value = request['value']
                    response = {
                        "type": MessageType.PUT_RESPONSE,
                        "key": key,
                        "value": socket_node.increment_item(key, value),
                        "address": socket.IDENTITY.decode('utf-8'),
                        "quorum_id": request['quorum_id']
                    }
                    # time.sleep(1)
                    socket.send_json(response)
                    continue

            # request = socket.recv( zmq.NOBLOCK
            # Receive the response from the appropriate node using the ZeroMQ socket for that node.

            # Send the response to the client.

            socket.send(json.dumps({key: "bar"}).encode('utf-8'))

    def process_tasks(self, tasks_queue, read_quorum_requests_state, write_quorum_requests_state):
        """For requests and replies between the router and server nodes"""
        while True:
            task = tasks_queue.get()
            if task is None:
                break

            match get_request_type(task):
                case MessageType.GET_RESPONSE:
                    #     get responses have a id attribute
                    request_id = task['quorum_id']
                    if request_id in read_quorum_requests_state:
                        if task['address'] not in read_quorum_requests_state[request_id]['nodes_with_reply']:
                            # if the request is in the read quorum requests state, add the response to the responses list
                            read_quorum_requests_state[request_id]['nodes_with_reply'].add(task['address'])
                            read_quorum_requests_state[request_id]['responses'].append(task['value'])
                            read_quorum_requests_state[request_id]['retry_info'][task['address']] += 1
                    else:
                        print("GET::Received a response for a request that was already processed")
                        continue

                case MessageType.PUT_RESPONSE:
                    request_id = task['quorum_id']
                    if request_id in write_quorum_requests_state:
                        # print("PUT::Received a new response")
                        # if the request is in the read quorum requests state, add the response to the responses list
                        if task['address'] not in write_quorum_requests_state[request_id]['nodes_with_reply']:
                            write_quorum_requests_state[request_id]['nodes_with_reply'].add(task['address'])
                            write_quorum_requests_state[request_id]['responses'].append(task['value'])
                            write_quorum_requests_state[request_id]['retry_info'][task['address']] += 1
                    else:
                        print("PUT::Received a response for a request that was already processed")
                        print(task)
                        continue

                case MessageType.REGISTER:
                    print("REGISTER::Received a new node", task['address'])
                    self.add_node(task['address'])
                    # send a response to the node
                    self.router_socket.send_multipart([task['address'].encode('utf-8'),
                                                       json.dumps(build_register_response("ok")).encode('utf-8')])
                    continue


    def expose(self):
        # Start a thread to listen for requests on the client socket.
        tasks_thread = threading.Thread(target=self.process_tasks,
                                        args=(self.tasks_queue, self.read_quorum_requests_state,
                                              self.write_quorum_requests_state))
        tasks_thread.start()

        main_thread = threading.Thread(target=self.listen_for_client_requests)
        main_thread.start()

    def listen_for_client_requests(self):
        while True:
            identity, message = self.router_socket.recv_multipart()
            json_request = json.loads(message)
            if json_request['type'] == MessageType.GET_RESPONSE or json_request['type'] == MessageType.PUT_RESPONSE or json_request['type'] == MessageType.REGISTER:
                self.tasks_queue.put(json_request)
                continue

            request = message.decode('utf-8')

            # planning more cases, but only get for now
            # assume request is a json with key and request type
            if json_request['type'] == MessageType.GET:
                key = json_request['key']
                # Receive the response from the appropriate node using the ZeroMQ socket for that node.
                # Send the response to the client.

                value = self.get(key)
                self.router_socket.send_multipart([identity, value])

            elif json_request['type'] == 'put':
                shopping_list = json_request['shopping_list']
                # Receive the response from the appropriate node using the ZeroMQ socket for that node.
                # Send the response to the client.

                self.put(shopping_list)
                # list_id = shopping_list['id']

                # value = self.get(key)
                # self.router_socket.send_multipart([identity, value])

            # self.router_socket.send(response)

    def read_data_quorum(self, key):
        primary_node = self.hash_ring.get_node(key)
        replicas = self.hash_ring.get_replica_nodes(primary_node)

        quorum_id = str(uuid4())
        request = build_quorum_get_request(key, quorum_id)

        result = self.send_get_request_to_nodes(request, [primary_node] + replicas, 10, 3, R_QUORUM)

        if len(result) < R_QUORUM:
            print(result)
            return None
        # todo validate if all results are the same
        return result

    def write_data_quorum(self, key, value):
        primary_node = self.hash_ring.get_node(key)
        replicas = self.hash_ring.get_replica_nodes(primary_node)

        # the responses will arrive asynchronously, so we need to wait for them
        quorum_id = str(uuid4())
        request = build_quorum_put_request(key, value, quorum_id)

        result = self.send_put_request_to_nodes(request, [primary_node] + replicas, 5, 1, W_QUORUM)

        if len(result) < W_QUORUM:
            return None
        # todo validate if all results are the same
        return result

    def send_request_to_nodes(self, type, request, nodes, timeout, max_retries, quorum_size):
        quorum_id = request['quorum_id']
        if type == MessageType.GET:
            self.read_quorum_requests_state[quorum_id] = build_quorum_request_state(nodes, timeout, max_retries,
                                                                                    quorum_size)
            current_quorum_state = self.read_quorum_requests_state[quorum_id]
        elif type == MessageType.PUT:
            self.write_quorum_requests_state[quorum_id] = build_quorum_request_state(nodes, timeout, max_retries,
                                                                                     quorum_size)
            current_quorum_state = self.write_quorum_requests_state[quorum_id]

        start_time = time.time()
        # TODO should the condition be the length of the most common response?
        while time.time() - start_time < timeout and len(current_quorum_state['responses']) < quorum_size:
            for node in nodes:
                if not any([current_quorum_state['retry_info'][node] <= max_retries for node in nodes]):
                    continue
                if node in current_quorum_state['nodes_with_reply']:
                    continue
                if current_quorum_state['retry_info'][node] > max_retries:
                    continue
                current_quorum_state['retry_info'][node] += 1

                self.router_socket.send_multipart([node.encode('utf-8'), json.dumps(request).encode('utf-8')])

        result = current_quorum_state['responses'].copy()
        if type == MessageType.GET:
            del self.read_quorum_requests_state[quorum_id]
        elif type == MessageType.PUT:
            del self.write_quorum_requests_state[quorum_id]
        return result

    def send_get_request_to_nodes(self, request, nodes, timeout, max_retries, quorum_size):
        return self.send_request_to_nodes(MessageType.GET, request, nodes, timeout, max_retries, quorum_size)

    def send_put_request_to_nodes(self, request, nodes, timeout, max_retries, quorum_size):
        return self.send_request_to_nodes(MessageType.PUT, request, nodes, timeout, max_retries, quorum_size)

    def route(self, list_id: str):
        return self.hash_ring.get_node(list_id)

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




def hash_ring_testing(hash_table):
    reversed_hash_table = {}
    for key, value in hash_table.hash_ring.ring.items():
        reversed_hash_table.setdefault(value, []).append(key)
    i = 0
    for node, keys in reversed_hash_table.items():
        plt.clf()
        plt.plot([hash_table.hash_ring.sorted_keys.index(x) for x in keys], [int(x, 16) for x in keys], '.', label=node)
        plt.show()
    plt.clf()
    for node, keys in reversed_hash_table.items():
        plt.plot([hash_table.hash_ring.sorted_keys.index(x) for x in keys], [int(x, 16) for x in keys], '.', label=node)
    plt.show()
    positions = []
    # for 1000 random strings of length 10, plot the distribution of the positions of the got node in the ring
    for i in range(100000):
        key = str(uuid4())
        positions.append(hash_table.hash_ring.get_node_pos(key))
        # print(get_node_pos(key))
    l = len(hash_table.hash_ring.sorted_keys)
    plt.clf()
    plt.hist(positions, bins=l)
    plt.show()
    print('Plotting done')


def main():
    nodes = [
        "tcp://localhost:5555",
        "tcp://localhost:5556",
        "tcp://localhost:5557",
        "tcp://localhost:5558",
    ]
    hash_table = Router(replica_count=24)
    hash_table.expose()

    print(f"Current encoding: {sys.stdin.encoding}")
    # change to latin-1
    sys.stdin = open(sys.stdin.fileno(), mode='r', encoding='latin-1', buffering=True)


    if input('Test the hash ring? (y/n) ').lower() == 'y':
        hash_ring_testing(hash_table)


    if input('Test quorums? (y/n) ').lower() == 'y':
        key_to_lookup = "key2"
        hash_table.write_data_quorum(key_to_lookup, 4)
        result = hash_table.read_data_quorum(key_to_lookup)
        print(
            f"---------------------------------------------------------------------------"
            f"\nData for key '{key_to_lookup}' with quorum: {result}\n"
            f"---------------------------------------------------------------------------\n")


if __name__ == "__main__":
    # asyncio.run(main())
    main()
