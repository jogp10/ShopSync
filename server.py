# Start of server.py
import json
import queue
import sys
from uuid import uuid4
import matplotlib.pyplot as plt

import zmq
import threading

from hash_ring import HashRing
from utils import *
from bstar_utils import *
import time


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

        self.nodes = nodes
        self.activity = {}  # for heartbeats and potentially other things
        self.hash_ring = HashRing(nodes, replica_count)
        self.tasks_queue = queue.Queue()
        self.heavy_tasks_queue = queue.Queue()

        # states of the quorums that have been forwarded to the nodes
        self.forwarded_read_quorums = {}  # a dictionary of quorum_id -> dictionary of node -> number of tries
        self.forwarded_write_quorums = {}  # a dictionary of quorum_id -> dictionary of node -> number of tries
        self.forwarded_delete_quorums = {}  # a dictionary of quorum_id -> dictionary of node -> number of tries
        # todo maybe store which nodes are busy at the moment


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
        print(f"Adding node {node_address}\n")
        for node in self.nodes:
            self.router_socket.send_multipart([node.encode('utf-8'),
                                               json.dumps(build_add_node_request(node_address)).encode('utf-8')])
        self.hash_ring.add_node(node_address)
        self.activity[node_address] = {}
        self.activity[node_address]['last_time_active'] = time.time()
        self.activity[node_address]['immediately_available'] = False
        self.nodes.append(node_address)

    def remove_node(self, node_address):
        for node in self.nodes:
            self.router_socket.send_multipart([node.encode('utf-8'),
                                               json.dumps(build_remove_node_request(node_address)).encode('utf-8')])
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
                    previous_nodes = self.nodes.copy()
                    self.add_node(task['address'])
                    # send a response to the node, include the current nodes to configure the hashring
                    self.router_socket.send_multipart([task['address'].encode('utf-8'),
                                                       json.dumps(build_register_response(previous_nodes)).encode('utf-8')])
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

        main_thread = threading.Thread(target=self.listen_for_requests)
        main_thread.start()

        monitor_thread = threading.Thread(target=self.monitor_nodes)
        monitor_thread.start()

    def listen_for_requests(self):
        """From clients and server nodes"""
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

        if type_request == MessageType.REGISTER or type_request == MessageType.HEARTBEAT_RESPONSE:
            self.fsm.event = NODE_MESSAGE
        else:
            self.fsm.event = CLIENT_REQUEST

        try:
            run_fsm(self.fsm)

            if json_request['type'] not in client_request_types and json_request['type'] != MessageType.REGISTER:
                print(self.activity)
                if identity.decode('utf-8') in self.activity:
                    self.activity[identity.decode('utf-8')]['last_time_active'] = time.time()
                    self.activity[identity.decode('utf-8')]['immediately_available'] = True
                else:
                    print("Received a request from an unknown node", identity.decode('utf-8'), file=sys.stderr)
            if json_request['type'] in client_request_types:
                print("PUT/GET::Received a new request: ", json_request)
                json_request['address'] = identity.decode('utf-8')
                self.heavy_tasks_queue.put(json_request)
            elif json_request['type'] != MessageType.HEARTBEAT_RESPONSE:
                self.tasks_queue.put(json_request)
        except BStarException:
            print("BStarException")
            pass


    def _process_request(self, key, value, client_address, request_type):
        primary_node, pos = self.hash_ring.get_node(key)
        replicas = self.hash_ring.get_replica_nodes(primary_node, pos)[0]
        coordinator = self.elect_coordinator(primary_node, replicas)

        if coordinator is None:
            # todo ?
            print("Coordinator is None", file=sys.stderr)
            return

        quorum_id = str(uuid4())
        forwarded_quorums = self._get_forwarded_quorums(request_type)
        forwarded_quorums[quorum_id] = {node: 0 for node in [primary_node] + replicas}
        forwarded_quorums[quorum_id][coordinator] += 1
        forwarded_quorums[quorum_id]['client_address'] = client_address
        # todo error checking here

        request = self._build_request(request_type, key, value, quorum_id)
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

    def _build_request(self, request_type, key, value, quorum_id):
        if request_type == MessageType.GET:
            return build_quorum_get_request(key, quorum_id)
        elif request_type == MessageType.PUT:
            return build_quorum_put_request(key, value, quorum_id)
        elif request_type == MessageType.DELETE:
            return build_quorum_delete_request(key, quorum_id)

    def elect_coordinator(self, primary_node, replicas):
        # send an heartbeat to all nodes
        nodes_to_probe = [primary_node] + list(set(replicas))
        print("\nNodes to probe: ", nodes_to_probe, "\n")
        for node in nodes_to_probe:
            self.activity[node]['immediately_available'] = False
            self.router_socket.send_multipart([node.encode('utf-8'), json.dumps(build_heartbeat_request()).encode('utf-8')])
        coordinator = None

        start_time = time.time()
        while time.time() - start_time < COORDINATOR_HEALTH_CHECK_TIMEOUT:
            for node in nodes_to_probe:
                if node in self.activity and valid_health_check(self.activity[node]):
                    coordinator = node
                    break

        if coordinator != primary_node:
            print("Coordinator is not the primary node", coordinator)
        #     check again if the primary node is alive
            if valid_health_check(self.activity[primary_node]):
                coordinator = primary_node
                print("ACTUALLY... Coordinator is the primary node")

        for node in nodes_to_probe:
            self.activity[node]['immediately_available'] = False
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
