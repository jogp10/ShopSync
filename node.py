import json
import queue
import sys
import threading
import os
import sqlite3
import atexit

import zmq

from crdt import ShoppingListCRDT, upsert_list
from hash_ring import HashRing
from shopping_list import ShoppingList
from utils import *


class DynamoNode:
    def __init__(self, name, data: dict = {}):
        self.name = name
        self.data: dict = data  # a dict k: list id, v: ShoppingList[]
        self.dirty: dict = {}  # a dict k: list id, v: True if dirty, False otherwise

        self.read_quorum_requests_state = {}
        self.write_quorum_requests_state = {}
        self.delete_quorum_requests_state = {}

        self.hash_ring: HashRing = HashRing()

        for key in data.keys():
            self.dirty[key] = False

    def remove_list_from_node(self, list_id):
        self.data.pop(list_id, None)
        self.dirty.pop(list_id, None)

    def write_data(self, list_id, shopping_list: ShoppingList):
        if not shopping_list:
            return False
        self.dirty[list_id] = True
        upsert_list(list_id, shopping_list, self.data)
        return True

    def read_data(self, list_id):
        if self.data.get(list_id) is None:
            return None
        else:
            list_name = self.data[list_id][0].name
            merged_crdt = ShoppingListCRDT.zero()
            for shopping_list in self.data[list_id]:
                merged_crdt = merged_crdt.merge(shopping_list.items)

            merged_data = ShoppingList(list_id, list_name, merged_crdt)
            self.data[list_id] = [merged_data]  # assuming no need to maintain history

            return json.dumps(merged_data, default=lambda x: x.__dict__)

    def delete_data(self, list_id):
        if self.data.get(list_id) is None:
            return None
        else:
            self.remove_list_from_node(list_id)
            self.delete_shopping_list_database(list_id)
            return True

    def create_database_and_table(self):
        transformed_name = self.name.replace(' ', '_').replace(":", "_").replace("/", "-")
        db_folder = f"db_server/{transformed_name}/"

        if not os.path.exists(db_folder):
            os.makedirs(db_folder)

        conn = sqlite3.connect(os.path.join(db_folder, "node.db"))
        cursor = conn.cursor()

        # Create the shopping_list table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS shopping_list (
                id TEXT PRIMARY KEY,
                name TEXT,
                items TEXT
            )
        ''')

        conn.commit()
        conn.close()

    def get_database_data(self):
        transformed_name = self.name.replace(' ', '_').replace(":", "_").replace("/", "-")
        db_folder = f"db_server/{transformed_name}/"

        if not os.path.exists(db_folder):
            self.create_database_and_table()
            return

        conn = sqlite3.connect(os.path.join(db_folder, "node.db"))
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM shopping_list")
        data = cursor.fetchall()

        conn.close()

        # Convert the JSON strings back to Python dictionaries

        for row in data:
            items_dict = json.loads(row[2])
            shopping_list = {
                "id": row[0],
                "name": row[1],
                "items": items_dict
            }
            self.write_data(shopping_list["id"], ShoppingList.from_dict(shopping_list))

    def merge_shopping_lists(self, shopping_lists):
        if len(shopping_lists) == 0:
            return None
        list_id = shopping_lists[0].id
        list_name = shopping_lists[0].name
        merged_crdt = ShoppingListCRDT.zero()
        for shopping_list in shopping_lists:
            merged_crdt = merged_crdt.merge(shopping_list.items)

        merged_data = ShoppingList(list_id, list_name, merged_crdt)
        self.data[list_id] = [merged_data]
        return merged_data

    def save_all_database_data(self):
        print("saving all database data")
        transformed_name = self.name.replace(' ', '_').replace(":", "_").replace("/", "-")
        db_folder = f"db_server/{transformed_name}/"
        conn = sqlite3.connect(os.path.join(db_folder, "node.db"))
        cursor = conn.cursor()

        # Delete all rows from the shopping_list table
        cursor.execute("DELETE FROM shopping_list")

        for shopping_lists in self.data.values():
            shopping_list = self.merge_shopping_lists(shopping_lists)
            shopping_list_dict = shopping_list.to_dict()
            items_json = shopping_list_dict["items"].to_json_string()

            cursor.execute("INSERT INTO shopping_list (id, name, items) VALUES (?, ?, ?)",
                           (shopping_list_dict["id"], shopping_list_dict["name"], items_json))

        conn.commit()
        conn.close()

    def save_shopping_list_database(self, list_id):
        transformed_name = self.name.replace(' ', '_').replace(":", "_").replace("/", "-")
        db_folder = f"db_server/{transformed_name}/"
        conn = sqlite3.connect(os.path.join(db_folder, "node.db"))
        cursor = conn.cursor()

        shopping_list = self.data[list_id][0]
        shopping_list_dict = shopping_list.to_dict()
        items_json = shopping_list_dict["items"].to_json_string()

        cursor.execute("INSERT or REPLACE INTO shopping_list (id, name, items) VALUES (?, ?, ?)",
                       (shopping_list_dict["id"], shopping_list_dict["name"], items_json))

        self.dirty[list_id] = False

        conn.commit()
        conn.close()

    def delete_shopping_list_database(self, list_id):
        transformed_name = self.name.replace(' ', '_').replace(":", "_").replace("/", "-")
        db_folder = f"db_server/{transformed_name}/"
        conn = sqlite3.connect(os.path.join(db_folder, "node.db"))
        cursor = conn.cursor()

        cursor.execute("DELETE FROM shopping_list WHERE id = ?", (list_id,))

        conn.commit()
        conn.close()


class Node:

    def __init__(self, node_address):
        router_addresses = [ROUTER_ADDRESS, ROUTER_BACKUP_ADDRESS]
        # Create a new context and socket for the node
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.node_sockets = [""] * len(router_addresses)
        self.address = node_address

        for i in range(len(router_addresses)):
            self.node_sockets[i] = self.context.socket(zmq.DEALER)
            self.node_sockets[i].setsockopt(zmq.IDENTITY, node_address.encode('utf-8'))
            self.node_sockets[i].connect(router_addresses[i])
            # send a first message to the router to register the node
            self.node_sockets[i].send_json(build_register_request(node_address))

            self.poller.register(self.node_sockets[i], zmq.POLLIN)

        self.reply_socket = self.context.socket(zmq.PULL)
        self.reply_socket.setsockopt(zmq.IDENTITY, node_address.encode('utf-8'))
        self.reply_socket.bind(f"tcp://*:{port}")

        self.write_quorum_requests_state = {}
        self.read_quorum_requests_state = {}
        self.delete_quorum_requests_state = {}
        self.coordinated_quorums_queue = queue.Queue()

        # Create a DynamoNode instance for this node
        self.dynamo_node = DynamoNode(node_address)

        self.nodes_health = {}
        self.write_hints = {}
        self.delete_hints = {}


    def start(self):
        self.dynamo_node.get_database_data()

        # Deal with requests added tp the queue
        coordinated_quorums_thread = threading.Thread(target=self.coordinate_quorums,
                                                      args=(self.coordinated_quorums_queue,))
        coordinated_quorums_thread.start()

        # Deal with messages from other nodes
        reply_thread = threading.Thread(target=self.listen_for_nodes)
        reply_thread.start()

        #every 15 seconds so as to handoff stored hints
        t = time.time()

        # Start listening for messages from the ROUTER
        while True:
            try:
                socks = dict(self.poller.poll())
            except zmq.error.ZMQError:
                print("Error")
                break

            for socket in self.node_sockets:
                if (socks.get(socket) == zmq.POLLIN):
                    self.handle_server_request(socket)

            if time.time() - t > HINT_CHECK_INTERVAL:
                # set of keys in both dicts
                print(f"\n HINTS: {self.write_hints} \n {self.delete_hints} \n")
                failed_nodes = set(self.write_hints.keys()) | set(self.delete_hints.keys())
                # check health of failed nodes
                healthy, _ = self.check_nodes_health(failed_nodes)
                print(f"Healthy nodes: {healthy}"
                      f"Write hints: {self.write_hints}")
                #check for hints
                nodes_hint_to_delete = []
                for node in self.write_hints.keys():
                    if node in healthy:
                        print(f"Sending hand offs to {node}")
                        for key in self.write_hints[node]:
                            print(f"Sending hand offs for key {key}")
                            self.send_push_message(self.address, node,
                                                   json.dumps(build_put_handed_off_request(key,
                                                  self.read_data(key)), default=lambda x: x.__dict__))
                            self.delete_list_if_not_owned(key)
                        nodes_hint_to_delete.append(node)

                [self.write_hints.pop(node, None) for node in nodes_hint_to_delete]


                nodes_hint_to_delete = []
                for node in self.delete_hints.keys():
                    if node in healthy:
                        for key in self.delete_hints[node]:
                            self.send_push_message(self.address, node,
                                                   json.dumps(build_delete_handed_off_request(key)))
                        nodes_hint_to_delete.append(node)
                [self.delete_hints.pop(node, None) for node in nodes_hint_to_delete]


                t = time.time()


    def handle_server_request(self, socket):

        request = socket.recv_json()
        request["origin"] = socket
        print(request)

            # switch case on request type
        match get_request_type(request):

            case MessageType.COORDINATE_PUT:
                self.coordinated_quorums_queue.put(request)
                
            case MessageType.COORDINATE_GET:
                self.coordinated_quorums_queue.put(request)

            case MessageType.COORDINATE_DELETE:
                self.coordinated_quorums_queue.put(request)

            case MessageType.REGISTER_RESPONSE:
                self.dynamo_node.hash_ring = HashRing([self.address] + request['nodes'])

            case MessageType.ADD_NODE:
                new_node = request['node']
                if new_node not in self.dynamo_node.hash_ring.get_all_nodes():

                    self.dynamo_node.hash_ring.add_node(new_node)

                    # check if any lists are stored in this node that should be handed off
                    for key in self.dynamo_node.data.keys():
                        if self.check_if_owned(key, new_node):
                            self.send_push_message(self.address, new_node,
                                                   json.dumps(build_put_handed_off_request(key,
                                                   self.read_data(key)), default=lambda x: x.__dict__))

                        self.delete_list_if_not_owned(key)

                    # healthy, _ = self.check_nodes_health(self.dynamo_node.hash_ring.get_other_nodes(self.address))
                    # if self.write_hints.get(new_node, None) in healthy:
                    #     for key in self.write_hints[new_node]:
                    #         self.send_push_message(self.address, new_node,
                    #                                json.dumps(build_put_handed_off_request(key,
                    #                                self.read_data(key)), default=lambda x: x.__dict__))
                    #         self.delete_list_if_not_owned(key)
                    #
                    # if self.delete_hints.get(new_node, None) in healthy:
                    #     for key in self.delete_hints[new_node]:
                    #         self.send_push_message(self.address, new_node,
                    #                                json.dumps(build_delete_handed_off_request(key)))



            case MessageType.REMOVE_NODE:
                self.dynamo_node.hash_ring.remove_node(request['node'])
            #     remove from hints if present
                self.write_hints.pop(request['node'], None)
                self.delete_hints.pop(request['node'], None)

            #     delete any nodes from this dynamo node that are not owned
        # TODO when a node is deleted each node can be entitle to more lists, is this feasible?

            case MessageType.HEARTBEAT:
                response = {
                    "type": MessageType.HEARTBEAT_RESPONSE,
                    "address": socket.IDENTITY.decode('utf-8')
                }
                socket.send_json(response)

    def send_push_message(self, sender_address, receiver_address, message):
        """sender_address comes already encoded"""
        print(f"Sending push message from {sender_address} to {receiver_address}: {message}")
        # if sender_address is string, convert to bytes
        if isinstance(sender_address, str):
            sender_address = sender_address.encode('utf-8')
        if isinstance(receiver_address, bytes):
            receiver_address = receiver_address.decode('utf-8')
        push_socket = self.context.socket(zmq.PUSH)
        push_socket.connect(receiver_address)
        push_socket.send_multipart([sender_address, message.encode('utf-8')])
        push_socket.close()

    def send_request_to_other_nodes(self, request_type, request, nodes, timeout, max_retries, quorum_size):

        quorum_id = request['quorum_id']
        quorum_size = min(quorum_size, len(set(nodes)))
        print(f"Sending request to other nodes with quorum size {quorum_size}")
        if request_type == MessageType.GET:
            self.read_quorum_requests_state[quorum_id] = build_quorum_request_state(nodes, timeout, max_retries,
                                                                                    quorum_size)
            current_quorum_state = self.read_quorum_requests_state[quorum_id]
        if request_type == MessageType.PUT:
            self.write_quorum_requests_state[quorum_id] = build_quorum_request_state(nodes, timeout, max_retries,
                                                                                     quorum_size)
            current_quorum_state = self.write_quorum_requests_state[quorum_id]
        elif request_type == MessageType.DELETE:
            self.delete_quorum_requests_state[quorum_id] = build_quorum_request_state(nodes, timeout, max_retries,
                                                                                      quorum_size)
            current_quorum_state = self.delete_quorum_requests_state[quorum_id]

        start_time = time.time()
        while time.time() - start_time < timeout and len(current_quorum_state['responses']) < quorum_size:
            for node in nodes:
                if not any([current_quorum_state['retry_info'][node] <= max_retries for node in nodes]):
                    continue
                if node in current_quorum_state['nodes_with_reply']:
                    continue
                if current_quorum_state['retry_info'][node] > max_retries:
                    continue
                if time.time() - current_quorum_state['last_retry_time'][node] < MIN_TIME_BETWEEN_RETRIES:
                    continue
                current_quorum_state['retry_info'][node] += 1
                current_quorum_state['last_retry_time'][node] = time.time()

                self.send_push_message(self.address, node,
                                       json.dumps(request))

        result = current_quorum_state['responses'].copy()
        if request_type == MessageType.GET:
            del self.read_quorum_requests_state[quorum_id]
        elif request_type == MessageType.PUT:
            del self.write_quorum_requests_state[quorum_id]
        elif request_type == MessageType.DELETE:
            del self.delete_quorum_requests_state[quorum_id]
        return result

    def send_put_request_to_other_nodes(self, request, nodes, timeout, max_retries, quorum_size):
        return self.send_request_to_other_nodes(MessageType.PUT, request, nodes, timeout, max_retries, quorum_size)

    def send_get_request_to_other_nodes(self, request, nodes, timeout, max_retries, quorum_size):
        return self.send_request_to_other_nodes(MessageType.GET, request, nodes, timeout, max_retries, quorum_size)

    def send_delete_request_to_other_nodes(self, request, nodes, timeout, max_retries, quorum_size):
        return self.send_request_to_other_nodes(MessageType.DELETE, request, nodes, timeout, max_retries, quorum_size)

    def check_nodes_health(self, nodes):
        nodes_set = set(nodes)
        for node in nodes_set:
            self.send_push_message(self.address, node, json.dumps({"type": MessageType.HEALTH_CHECK}))

        start_time = time.time()
        all_ok = False
        while time.time() - start_time < HEALTH_CHECK_TIMEOUT:
            if all([self.nodes_health.get(node, False) for node in nodes_set]):
                all_ok = True
                break

        if not all_ok:
            print("Some nodes are not healthy.", file=sys.stderr)
            print("Unhealthy nodes: ")
            [print(node) for node in nodes_set if not self.nodes_health.get(node, False)]

        healthy = []
        unhealthy = []
        [healthy.append(node) if self.nodes_health.get(node, False) else unhealthy.append(node) for node in nodes_set]
        for node in nodes_set: self.nodes_health[node] = False

        return healthy, unhealthy


    def coordinate_quorums(self, tasks_queue):
        print("coordinating quorums")
        while True:
            task = tasks_queue.get()
            if task is None:  # poison pill
                break

            match task["type"]:
                case MessageType.COORDINATE_PUT:
                    quorum_id = task["quorum_id"]
                    key = task["key"]
                    value = task["value"]
                    primary_node, primary_node_index = self.dynamo_node.hash_ring.get_node(key)

                    if primary_node != self.address:
                        print("I am not the primary node for this key", file=sys.stderr)
                        # assuming hash ring is updated
                        upsert_list(primary_node, key, self.write_hints)


                    healthy, unhealthy = self.check_nodes_health(self.dynamo_node.hash_ring.get_other_nodes(self.address))
                    replicas, failed, substitutes = self.dynamo_node.hash_ring.get_replica_nodes(primary_node, primary_node_index, unhealthy)
                    print(f"Replicas: {replicas}, failed: {failed}, substitutes: {substitutes}")
                    substitutes = [substitute for substitute in substitutes if substitute != self.address]
                    if len(failed) > 0:
                        print(f"Failed count: {len(failed)}")
                        # send message to substitutes for the failed nodes
                        for idx, substitute in enumerate(substitutes[:len(failed)]):
                            self.send_push_message(self.address, substitute,
                                                   json.dumps(build_write_hint_request(key, failed[idx])))



                    server_socket = task["origin"]
                    request_to_replicas = build_put_request(key, value, quorum_id)

                    result = ([self.dynamo_node.write_data(key, ShoppingList.from_dict(json.loads(value)))] +
                              self.send_put_request_to_other_nodes(request_to_replicas, replicas, 5, 1,
                                                                   W_QUORUM))

                    print(result)
                    quorum_size = min(W_QUORUM, len(set(replicas)) + 1)
                    if len(result) < quorum_size:
                        result = False
                    else:
                        result = True
                    print("Result after quorum consensus: ", result)

                    response_to_router = build_quorum_put_response(quorum_id, result)
                    server_socket.send_json(response_to_router)

                case MessageType.COORDINATE_GET:
                    quorum_id = task["quorum_id"]
                    key = task["key"]

                    primary_node, primary_node_index = self.dynamo_node.hash_ring.get_node(key)

                    if primary_node != self.address:
                        print("I am not the primary node for this key", file=sys.stderr)
                        # assuming hash ring is updated, the node is just down for the moment, no action

                    healthy, unhealthy = self.check_nodes_health(
                        self.dynamo_node.hash_ring.get_other_nodes(self.address))
                    replicas, failed, substitutes = self.dynamo_node.hash_ring.get_replica_nodes(primary_node,
                                                                                                 primary_node_index,
                                                                                                 unhealthy)

                    replicas = [replica for replica in replicas if replica != self.address]
                    server_socket = task["origin"]
                    request_to_replicas = build_get_request(key, quorum_id)

                    tmp_result = ([self.read_data(key)] +
                              self.send_get_request_to_other_nodes(request_to_replicas, replicas, 5, 1,
                                                                   R_QUORUM))

                    print(tmp_result)
                    # exclude Nones from the list
                    result = [item for item in tmp_result if item is not None]
                    quorum_size = min(R_QUORUM, len(set(replicas)) + 1)  # there is no guarantee that the failed nodes will be able to reply
                    if len(result) < quorum_size:
                        result = False
                    else:
                        shopping_lists = [ShoppingList.from_dict(json.loads(shopping_list)) for shopping_list in result]
                        merged_shopping_list = self.dynamo_node.merge_shopping_lists(shopping_lists)
                        if merged_shopping_list is None:
                            result = False
                        else:
                            result = json.dumps(merged_shopping_list, default=lambda x: x.__dict__)
                    print("Result  after quorum consensus: ", result)

                    response_to_router = build_quorum_get_response(quorum_id, result)
                    server_socket.send_json(response_to_router)

                case MessageType.COORDINATE_DELETE:
                    quorum_id = task["quorum_id"]
                    key = task["key"]

                    primary_node, primary_node_index = self.dynamo_node.hash_ring.get_node(key)

                    if primary_node != self.address:
                        print("I am not the primary node for this key", file=sys.stderr)
                        # assume primary node is down and hash ring is correct,
                        # so we create a delete hint for the primary node
                        upsert_list(primary_node, key, self.delete_hints)

                    healthy, unhealthy = self.check_nodes_health(
                        self.dynamo_node.hash_ring.get_other_nodes(self.address))
                    replicas, failed, substitutes = self.dynamo_node.hash_ring.get_replica_nodes(primary_node,
                                                                                                 primary_node_index,
                                                                                                 unhealthy)

                    substitutes = [substitute for substitute in substitutes if substitute != self.address]

                    if len(failed) > 0:
                        print(f"Failed count: {len(failed)}")
                        # send message to substitutes for the failed nodes
                        for idx, substitute in enumerate(substitutes[:len(failed)]):
                            self.send_push_message(self.address, substitute,
                                                   json.dumps(build_delete_hint_request(key, failed[idx])))

                    request_to_replicas = build_delete_request(key, quorum_id)

                    result = ([self.dynamo_node.delete_data(key)] +
                              self.send_delete_request_to_other_nodes(request_to_replicas, replicas, 5, 1,
                                                                      R_QUORUM))

                    print(result)
                    quorum_size = min(R_QUORUM, len(set(replicas)) + 1 - len(failed))  # there is no guarantee that the substitute nodes had the list
                    if len(result) < quorum_size:
                        result = False
                    else:
                        result = True

                    server_socket = task["origin"]
                    response_to_router = build_quorum_delete_response(quorum_id, result)
                    server_socket.send_json(response_to_router)


    def handle_request_response(self, request_type_quorums_state, json_request):
        request_id = json_request['quorum_id']
        if request_id == NULL_QUORUM_ID:
            # write the value with dynamo node
            if json_request['value']:
                print(f"Receiving missing list {json_request}")
                self.dynamo_node.write_data(json_request['key'], ShoppingList.from_dict(json.loads(json_request['value'])))
        elif request_id in request_type_quorums_state:
            if json_request['address'] not in request_type_quorums_state[request_id]['nodes_with_reply']:
                request_type_quorums_state[request_id]['nodes_with_reply'].add(json_request['address'])
                request_type_quorums_state[request_id]['responses'].append(json_request['value'])
                request_type_quorums_state[request_id]['retry_info'][json_request['address']] += 1
        else:
            print(f"{request_type_quorums_state}::Received a response for a request that was already processed")

    def handle_request(self, message_type, json_request, sender_identity):
        key = json_request['key']
        if message_type == MessageType.PUT:
            value = json_request['value']
            value = self.dynamo_node.write_data(key, ShoppingList.from_dict(json.loads(value)))
        elif message_type == MessageType.GET:
            value = self.read_data(key)
        elif message_type == MessageType.DELETE:
            value = self.dynamo_node.delete_data(key)

        response = {
            "type": message_type + "_RESPONSE",
            "key": key,
            "value": value,
            "address": self.address,
            "quorum_id": json_request['quorum_id']
        }
        self.send_push_message(self.address, sender_identity, json.dumps(response))

    def listen_for_nodes(self):
        """Messages from other nodes, get, put, delete received from a coordinator or the coordinator itself receives _responses"""
        print("listening for nodes")
        while True:
            sender_identity, message = self.reply_socket.recv_multipart()
            json_request = json.loads(message)
            print(sender_identity)
            print(json_request)
            print('\n\n')

            match get_request_type(json_request):
                case MessageType.PUT_RESPONSE:
                    self.handle_request_response(self.write_quorum_requests_state, json_request)
                    continue

                case MessageType.GET_RESPONSE:
                    self.handle_request_response(self.read_quorum_requests_state, json_request)
                    continue

                case MessageType.DELETE_RESPONSE:
                    self.handle_request_response(self.delete_quorum_requests_state, json_request)
                    continue

                case MessageType.PUT:
                    self.handle_request(MessageType.PUT, json_request, sender_identity)
                    continue

                case MessageType.GET:
                    self.handle_request(MessageType.GET, json_request, sender_identity)
                    continue

                case MessageType.DELETE:
                    self.handle_request(MessageType.DELETE, json_request, sender_identity)
                    continue

                case MessageType.WRITE_HINT:
                    # store the hint
                    print("RECEIVING WRITE-HINT FROM ", sender_identity.decode('utf-8'))
                    key = json_request['key']
                    node = json_request['node']
                    upsert_list(node, key, self.write_hints)

                case MessageType.DELETE_HINT:
                    # store the hint
                    key = json_request['key']
                    node = json_request['node']
                    upsert_list(node, key, self.delete_hints)

                case MessageType.PUT_HANDED_OFF:
                    key = json_request['key']
                    value = json_request['value']
                    self.dynamo_node.write_data(key, ShoppingList.from_dict(json.loads(value)))
                    continue

                case MessageType.DELETE_HANDED_OFF:
                    key = json_request['key']
                    self.dynamo_node.delete_data(key)
                    continue

                case MessageType.HEALTH_CHECK:
                    # If a HEALTH_CHECK message is received, send a HEALTH_CHECK_RESPONSE message
                    response = {"type": MessageType.HEALTH_CHECK_RESPONSE}
                    self.send_push_message(self.address, sender_identity,
                                           json.dumps(response))
                    continue

                case MessageType.HEALTH_CHECK_RESPONSE:
                    self.nodes_health[sender_identity.decode('utf-8')] = True
                    continue

    def update_health_statuses(self, healthy, unhealthy):
        for node in unhealthy:
            self.nodes_health[node] = False

        for node in healthy:
            if not self.nodes_health.get(node, False):
                pass
            self.nodes_health[node] = True

    def get_unhealthy_nodes(self, nodes):
        return [node for node in nodes if not self.nodes_health.get(node, False)]

    def delete_list_if_not_owned(self, key):
        primary_node, primary_node_pos = self.dynamo_node.hash_ring.get_node(key)
        replicas = self.dynamo_node.hash_ring.get_ideal_replica_nodes(primary_node, primary_node_pos)
        if self.address not in replicas:
            self.dynamo_node.delete_data(key)

    def check_if_owned(self, key, node=None):
        if node is None:
            node = self.address
        primary_node, primary_node_pos = self.dynamo_node.hash_ring.get_node(key)
        replicas = self.dynamo_node.hash_ring.get_ideal_replica_nodes(primary_node, primary_node_pos)
        return node in replicas

    def read_data(self, key):
        result = self.dynamo_node.read_data(key)
        if result is None and self.check_if_owned(key):
            # ask other nodes for the list
            primary_node, primary_node_pos = self.dynamo_node.hash_ring.get_node(key)
            replicas, _, _ = self.dynamo_node.hash_ring.get_replica_nodes(primary_node, primary_node_pos)
            replicas = [replica for replica in replicas if replica != self.address]
            if len(replicas) > 0:
                chosen_replica = replicas[0]
                print(f"Asking {chosen_replica} for missing list")
                self.send_push_message(self.address, chosen_replica,
                                       json.dumps(build_get_request(key, NULL_QUORUM_ID)))

        return result


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python node.py <port>")
        sys.exit(1)

    port = sys.argv[1]
    node_address = f"tcp://localhost:{port}"

    node: Node = Node(node_address)

    # ensure that at exit the database is saved
    atexit.register(node.dynamo_node.save_all_database_data)

    node.start()
