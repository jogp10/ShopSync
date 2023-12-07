# Start of server.py
import hashlib
import json
import queue
import sys
from uuid import uuid4
import matplotlib.pyplot as plt

import zmq
import threading
from utils import *
from bstar_utils import *
import time


class HashRing:
    def __init__(self, nodes, replica_count):
        self.ring = {}
        self.replica_count = replica_count
        self.sorted_keys = []
        self.generate_ring(nodes)

    def generate_ring(self, nodes):
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
            index = (index + 1) % len(self.sorted_keys)
            j += 1

        return [self.ring[self.sorted_keys[i]] for i in replica_indices]

    def hash_key(self, key):
        return hashlib.sha256(key.encode()).hexdigest()

    def add_node(self, node):
        # TODO probably more to do here
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

    '''
    primary: bool -> True if the primary router, false if the backup
    address: str -> str with bind address
    nodes: list -> list of nodes (default [] and router discovers the nodes)
    replica_count: int -> number of replicas
    '''
    
    def __init__(self, primary: bool, address: str, nodes=[], replica_count=3):
        self._context = zmq.Context()

        self.fsm = BStarState(0, 0, 0)

        
        if primary:
            print("I: Primary master, waiting for backup (slave)")
            self.fsm.state = STATE_PRIMARY
            self.local_socket = ROUTER_ADDRESS_LOCAL_PEER
            self.remote_socket = ROUTER_ADDRESS_REMOTE_PEER
        else:
            print("I: Backup slave, waiting for primary (master)")
            self.fsm.state = STATE_BACKUP
            self.local_socket = ROUTER_BACKUP_ADDRESS_LOCAL_PEER
            self.remote_socket = ROUTER_BACKUP_ADDRESS_REMOTE_PEER

        self.sockets = {}
        self.threads = {}
        self.nodes = nodes
        self.activity = {}  # for heartbeats and potentially other things
        self.hash_ring = HashRing(nodes, replica_count)
        self.tasks_queue = queue.Queue()
        self.heavy_tasks_queue = queue.Queue()  # todo ter os nós a coordenar os quorums evita esta queue
        self.read_quorum_requests_state = {}
        # self.write_quorum_requests_state = {}
        self.delete_quorum_requests_state = {}

        # states of the quorums that have been forwarded to the nodes
        self.forwarded_read_quorums = {}  # a dictionary of quorum_id -> dictionary of node -> number of tries
        self.forwarded_write_quorums = {}  # a dictionary of quorum_id -> dictionary of node -> number of tries, however
        self.forwarded_delete_quorums = {}  # a dictionary of quorum_id -> dictionary of node -> number of tries, however
        # forwarded_write_quorums[quroum_id][client_address]  is the address of the client that sent the request
        # todo maybe store which nodes are busy at the moment

        # hinted hand-off todo

        #ROUTER ATTR
        self.statepub = self._context.socket(zmq.PUB)
        self.statesub = self._context.socket(zmq.SUB)
        self.statesub.setsockopt_string(zmq.SUBSCRIBE, "")

        router_address = address
        self.router_socket = self._context.socket(zmq.ROUTER)
        self.router_socket.bind(router_address)

        self.statepub.bind(self.local_socket)
        self.statesub.connect(self.remote_socket)
        self.statesub.setsockopt_string(zmq.SUBSCRIBE, "")

        self.poller = zmq.Poller()
        self.poller.register(self.router_socket, zmq.POLLIN)
        self.poller.register(self.statesub, zmq.POLLIN)

        self.send_state_at = int(time.time() * 1000 + HEARTBEAT_BSTAR)


    def add_node(self, node_address):
        print(f"Adding node {node_address}")
        self.hash_ring.add_node(node_address)
        self.activity[node_address] = {}
        self.activity[node_address]['last_time_active'] = time.time()
        self.nodes.append(node_address)
        print(self.nodes)

    def remove_node(self, node_address):
        self.activity.pop(node_address)
        self.nodes.remove(node_address)
        self.hash_ring.remove_node(node_address)

    def listen(self, socket: zmq.Socket):
        pass
        # this function is stll here at the moment in case it easier to test multiple nodes as threads
        # dont delete just yet
        # identity = socket.IDENTITY.decode('utf-8')
        # socket_node = DynamoNode(identity)

    def process_tasks(self, tasks_queue):
        """For requests and replies between the router and server nodes"""
        while True:
            task = tasks_queue.get()
            if task is None:
                break

            match get_request_type(task):

                case MessageType.REGISTER:
                    print("REGISTER::Received a new node", task['address'])
                    self.add_node(task['address'])
                    # send a response to the node
                    self.router_socket.send_multipart([task['address'].encode('utf-8'),
                                                       json.dumps(build_register_response("ok")).encode('utf-8')])
                    continue

                case MessageType.COORDINATE_PUT_RESPONSE:
                    # print("COORDINATE_PUT_RESPONSE::Received a new response")
                    # if the request is in the read quorum requests state, add the response to the responses list
                    request_id = task['quorum_id']
                    print(task)
                    if request_id in self.forwarded_write_quorums:
                        result = task['result']
                        self.router_socket.send_multipart(
                            [self.forwarded_write_quorums[request_id]['client_address'].encode('utf-8'),
                             json.dumps(result).encode('utf-8')])
                        # send to client
                        del self.forwarded_write_quorums[request_id]
                    else:
                        print("COORDINATE_PUT_RESPONSE::Received a response for a request that was already processed")
                        print(task)
                        continue

                case MessageType.COORDINATE_DELETE_RESPONSE:
                    # print("COORDINATE_PUT_RESPONSE::Received a new response")
                    # if the request is in the read quorum requests state, add the response to the responses list
                    request_id = task['quorum_id']
                    print(task)
                    if request_id in self.forwarded_delete_quorums:
                        result = task['result']
                        self.router_socket.send_multipart(
                            [self.forwarded_delete_quorums[request_id]['client_address'].encode('utf-8'),
                             json.dumps(result).encode('utf-8')])
                        # send to client
                        del self.forwarded_delete_quorums[request_id]
                    else:
                        print("COORDINATE_PUT_RESPONSE::Received a response for a request that was already processed")
                        print(task)
                        continue

                case MessageType.COORDINATE_GET_RESPONSE:
                    # print("COORDINATE_PUT_RESPONSE::Received a new response")
                    # if the request is in the read quorum requests state, add the response to the responses list
                    request_id = task['quorum_id']
                    print(task)
                    if request_id in self.forwarded_read_quorums:
                        result = task['result']
                        self.router_socket.send_multipart(
                            [self.forwarded_read_quorums[request_id]['client_address'].encode('utf-8'),
                             json.dumps(result).encode('utf-8')])
                        # send to client
                        del self.forwarded_read_quorums[request_id]
                    else:
                        print("COORDINATE_PUT_RESPONSE::Received a response for a request that was already processed")
                        print(task)
                        continue

    def process_heavy_tasks(self, tasks_queue):
        """For requests and replies between the client and server nodes"""
        while True:
            task = tasks_queue.get()
            if task is None:
                break

            match get_request_type(task):
                case MessageType.PUT:
                    shopping_list = task['value']
                    key = task['key']
                    self.put(key, shopping_list, task['address'])

                case MessageType.GET:
                    key = task['key']
                    self.get(key, task['address'])

                case MessageType.DELETE:
                    key = task['key']
                    self.delete(key, task['address'])

    def monitor_nodes(self):
        # Periodically check for node heartbeats
        while True:
            for node, info in list(self.activity.items()):
                if not valid_heartbeat(info):
                    # Node is considered down
                    print(f"Node {node} is down.")
                    self.remove_node(node)
                else:
                    pass

            # Sleep for a short interval before the next check
            time.sleep(MONITOR_INTERVAL)

            # send a heartbeat to all nodes
            for node in self.nodes:
                self.router_socket.send_multipart(
                    [node.encode('utf-8'), json.dumps(build_heartbeat_request()).encode('utf-8')])

    def expose(self):
        # Start a thread to listen for requests on the client socket.
        tasks_thread = threading.Thread(target=self.process_tasks,
                                        args=(self.tasks_queue,))
        tasks_thread.start()

        heavy_tasks_thread = threading.Thread(target=self.process_heavy_tasks,
                                              args=(self.heavy_tasks_queue,))

        heavy_tasks_thread.start()

        main_thread = threading.Thread(target=self.listen_for_client_requests)
        main_thread.start()

        monitor_thread = threading.Thread(target=self.monitor_nodes)
        monitor_thread.start()

    def listen_for_client_requests(self):
        """Actually not just clients, needs another name"""
        while True:
            time_left = self.send_state_at - int(time.time() * 1000)
            if time_left < 0:
                time_left = 0
            try:
                socks = dict(self.poller.poll(time_left))
            except zmq.error.ZMQError:
                print("Error")
                break
            if socks.get(self.router_socket) == zmq.POLLIN:
                self.handle_router_message()
            
            if socks.get(self.statesub) == zmq.POLLIN:
                self.handle_state_message()

            if int(time.time() * 1000) >= self.send_state_at:
                self.send_state_message()

    def handle_state_message(self):
        msg = self.statesub.recv()
        self.fsm.event = int(msg)
        print(self.fsm.state, self.fsm.event)
        del msg
        try:
            run_fsm(self.fsm)
            self.fsm.peer_expiry = int(time.time() * 1000) + (2 * HEARTBEAT_BSTAR)
        except BStarException:
            pass


    def send_state_message(self):
        self.statepub.send_string("%d" % self.fsm.state)
        self.send_state_at = int(time.time() * 1000) + HEARTBEAT_BSTAR

    def handle_router_message(self):
        client_request_types = [MessageType.GET, MessageType.PUT, MessageType.DELETE]

        identity, message = self.router_socket.recv_multipart()
        json_request = json.loads(message)
        print(json_request)

        type_request = json_request['type']

        if type_request == MessageType.REGISTER:
            self.fsm.event = NODE_REGISTER
        else:
            self.fsm.event = CLIENT_REQUEST
        
        try:
            run_fsm(self.fsm)

            if json_request['type'] not in client_request_types and json_request['type'] != MessageType.REGISTER:
                if identity.decode('utf-8') in self.activity:
                    self.activity[identity.decode('utf-8')]['last_time_active'] = time.time()
            if json_request['type'] in client_request_types:
                print("PUT/GET::Received a new request: ", json_request)
                json_request['address'] = identity.decode('utf-8')
                self.heavy_tasks_queue.put(json_request)
            elif json_request['type'] != MessageType.HEARTBEAT_RESPONSE:
                self.tasks_queue.put(json_request)
        except BStarException:
            pass



    def send_request_to_nodes(self, type, request, nodes, timeout, max_retries, quorum_size):
        quorum_id = request['quorum_id']
        if type == MessageType.GET:
            self.read_quorum_requests_state[quorum_id] = build_quorum_request_state(nodes, timeout, max_retries,
                                                                                    quorum_size)
            current_quorum_state = self.read_quorum_requests_state[quorum_id]
        # elif type == MessageType.PUT:
        #     self.write_quorum_requests_state[quorum_id] = build_quorum_request_state(nodes, timeout, max_retries,
        #                                                                              quorum_size)
        #     current_quorum_state = self.write_quorum_requests_state[quorum_id]
        elif type == MessageType.DELETE:
            self.delete_quorum_requests_state[quorum_id] = build_quorum_request_state(nodes, timeout, max_retries,
                                                                                      quorum_size)
            current_quorum_state = self.delete_quorum_requests_state[quorum_id]

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
        # elif type == MessageType.PUT:
        #     del self.write_quorum_requests_state[quorum_id]
        elif type == MessageType.DELETE:
            del self.delete_quorum_requests_state[quorum_id]
        return result

    def send_get_request_to_nodes(self, request, nodes, timeout, max_retries, quorum_size):
        return self.send_request_to_nodes(MessageType.GET, request, nodes, timeout, max_retries, quorum_size)

    def send_put_request_to_nodes(self, request, nodes, timeout, max_retries, quorum_size):
        return self.send_request_to_nodes(MessageType.PUT, request, nodes, timeout, max_retries, quorum_size)

    def send_delete_request_to_nodes(self, request, nodes, timeout, max_retries, quorum_size):
        return self.send_request_to_nodes(MessageType.DELETE, request, nodes, timeout, max_retries, quorum_size)

    def route(self, list_id: str):
        return self.hash_ring.get_node(list_id)

    def _process_request(self, key, value, client_address, request_type):
        primary_node = self.hash_ring.get_node(key)
        replicas = self.hash_ring.get_replica_nodes(primary_node)
        coordinator = self.elect_coordinator(primary_node, replicas)

        if coordinator is None:
            # todo scream
            pass

        quorum_id = str(uuid4())
        forwarded_quorums = self._get_forwarded_quorums(request_type)
        forwarded_quorums[quorum_id] = {node: 0 for node in [primary_node] + replicas}
        forwarded_quorums[quorum_id][coordinator] += 1
        forwarded_quorums[quorum_id]['client_address'] = client_address

        other_nodes = [node for node in [primary_node] + replicas if node != coordinator]

        request = self._build_request(request_type, key, value, quorum_id, other_nodes)
        print("Sending request to coordinator: ", request, coordinator)
        self.router_socket.send_multipart([coordinator.encode('utf-8'), json.dumps(request).encode('utf-8')])

    def get(self, key, client_address):
        self._process_request(key, None, client_address, MessageType.GET)

    def put(self, key, value, client_address):
        self._process_request(key, value, client_address, MessageType.PUT)

    def delete(self, key, client_address):
        self._process_request(key, None, client_address, MessageType.DELETE)

    def _get_forwarded_quorums(self, request_type):
        if request_type == MessageType.GET:
            return self.forwarded_read_quorums
        elif request_type == MessageType.PUT:
            return self.forwarded_write_quorums
        elif request_type == MessageType.DELETE:
            return self.forwarded_delete_quorums

    def _build_request(self, request_type, key, value, quorum_id, other_nodes):
        if request_type == MessageType.GET:
            return build_quorum_get_request(key, quorum_id, other_nodes)
        elif request_type == MessageType.PUT:
            return build_quorum_put_request(key, value, quorum_id, other_nodes)
        elif request_type == MessageType.DELETE:
            return build_quorum_delete_request(key, quorum_id, other_nodes)

    def elect_coordinator(self, primary_node, replicas):
        coordinator = None
        for node in [primary_node] + replicas:
            if node in self.activity and valid_heartbeat(self.activity[node]):
                coordinator = node
                break
        return coordinator


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
    # Arguments can be either of:
    #     -p  primary server, at ROUTER_BIND_ADDRESS
    #     -b  backup server, at ROUTER_BACKUP_BIND_ADDRESS
    if '-p' in sys.argv:
        hash_table = Router(primary=True, address=ROUTER_BIND_ADDRESS, replica_count=24)
    elif '-b' in sys.argv:
        hash_table = Router(primary=False, address=ROUTER_BACKUP_BIND_ADDRESS, replica_count=24)
    else:
        print("Usage: server.py { -p | -b }\n")
        return

    hash_table.expose()

    print(f"Current encoding: {sys.stdin.encoding}")
    # change to latin-1
    sys.stdin = open(sys.stdin.fileno(), mode='r', encoding='latin-1', buffering=True)

    if input('Test the hash ring? (y/n) ').lower() == 'y':
        hash_ring_testing(hash_table)


if __name__ == "__main__":
    # asyncio.run(main())
    main()
