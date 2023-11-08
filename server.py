# Start of server.py

# Should we just use HTTP instead of ZeroMQ?


import hashlib
import json
import math
import time
from bisect import bisect
from uuid import uuid4

import zmq
import threading

class HashTable:
    def __init__(self, nodes):
        self._context = zmq.Context()
        self.sockets = {}
        self.threads = {}
        self.nodes = nodes
        self.read_quorum = len(nodes) // 2 + 1
        self.write_quorum = len(nodes) // 2 + 1

        self.ring = {}
        self.weights = {}

        self._sorted_keys = []
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

    def _hash_val(self, b_key, entry_fn):
        return ((b_key[entry_fn(3)] << 24)
                | (b_key[entry_fn(2)] << 16)
                | (b_key[entry_fn(1)] << 8)
                | b_key[entry_fn(0)])

    def _hash_digest(self, key):
        m = hashlib.md5()
        m.update(key.encode('utf-8'))
        return [ord(p) for p in m.hexdigest()]

    def gen_key(self, key):
        """Given a string key it returns a long value,
        this long value represents a place on the hash ring.
        md5 is currently used because it mixes well.
        """
        b_key = self._hash_digest(key)
        return self._hash_val(b_key, lambda x: x)

    def get_node(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned.
        If the hash ring is empty, `None` is returned.
        """
        pos = self.get_node_pos(string_key)
        if pos is None:
            return None
        return self.ring[self._sorted_keys[pos]]

    def get_node_pos(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned
        along with it's position in the ring.
        If the hash ring is empty, (`None`, `None`) is returned.
        """
        if not self.ring:
            return None

        key = self.gen_key(string_key)

        nodes = self._sorted_keys
        pos = bisect(nodes, key)

        if pos == len(nodes):
            return 0
        else:
            return pos


# TODO insert and delete node
    def generate_ring(self):
        """
        https://gist.github.com/soekul/a240f9e11d6439bd0237c4ab45dce7a2
        :return:
        """
        total_weight = 0
        node_range = 4
        for node in self.nodes:
            total_weight += self.weights.get(node, 1)

        for node in self.nodes:

            weight = self.weights.get(node, 1)

            factor = math.floor((node_range * len(self.nodes) * weight) / total_weight)

            for j in range(0, int(factor)):
                b_key = self._hash_digest('%s-%s' % (node, j))

                for i in range(0, 3):
                    key = self._hash_val(b_key, lambda x: x + i * 4)
                    self.ring[key] = node
                    self._sorted_keys.append(key)

        self._sorted_keys.sort()

        print(self._sorted_keys)
        print(self.ring)

    def listen(self, socket: zmq.Socket):
        while True:
            # TODO usar polling ou selector para verificar/escolher se tem mensagens

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


if __name__ == "__main__":
    nodes = [
        "tcp://localhost:5555",
        "tcp://localhost:5556",
        "tcp://localhost:5557",
        "tcp://localhost:5558",
    ]

    hash_table = HashTable(nodes)

    # Start listening for requests on the client socket.
    hash_table.expose()
    # hash_table.client_socket.listen()
    #
    # Get the value for the key "foo".
    # value = hash_table.get("foo")
    # print(value)
















