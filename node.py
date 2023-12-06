import json
import queue
import sys
import threading
import time
from typing import Dict
import os
import sqlite3
import atexit

import zmq

from crdt import ShoppingListCRDT, upsert_list
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

        for key in data.keys():
            self.dirty[key] = False

    def remove_list_from_node(self, list_id):
        self.data.pop(list_id, None)
        self.dirty.pop(list_id, None)

    def write_data(self, list_id, shopping_list: ShoppingList):
        self.dirty[list_id] = True
        upsert_list(list_id, shopping_list, self.data)
        return True  # False if errors?

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
        router_address = ROUTER_ADDRESS  # todo change if more routers
        # Create a new context and socket for the node
        self.context = zmq.Context()
        self.node_socket = self.context.socket(zmq.DEALER)
        self.node_socket.setsockopt(zmq.IDENTITY, node_address.encode('utf-8'))
        self.node_socket.connect(router_address)
        # send a first message to the router to register the node
        self.node_socket.send_json(build_register_request(node_address))

        self.reply_socket = self.context.socket(zmq.PULL)
        self.reply_socket.setsockopt(zmq.IDENTITY, node_address.encode('utf-8'))
        self.reply_socket.bind(f"tcp://*:{port}")

        self.write_quorum_requests_state = {}
        self.read_quorum_requests_state = {}
        self.delete_quorum_requests_state = {}
        self.coordinated_quorums_queue = queue.Queue()

        # Create a DynamoNode instance for this node
        self.dynamo_node = DynamoNode(node_address)

    def start(self):
        self.dynamo_node.get_database_data()

        # Deal with requests added tp the queue
        coordinated_quorums_thread = threading.Thread(target=self.coordinate_quorums,
                                                      args=(self.coordinated_quorums_queue,))
        coordinated_quorums_thread.start()

        # Deal with messages from other nodes
        reply_thread = threading.Thread(target=self.listen_for_nodes)
        reply_thread.start()

        # Start listening for messages from the router
        while True:
            request = self.node_socket.recv_json()
            print(request)

            # switch case on request type
            match get_request_type(request):

                case MessageType.COORDINATE_PUT:
                    self.coordinated_quorums_queue.put(request)
                    continue

                case MessageType.COORDINATE_GET:
                    self.coordinated_quorums_queue.put(request)
                    continue

                case MessageType.COORDINATE_DELETE:
                    self.coordinated_quorums_queue.put(request)
                    continue


                case MessageType.REGISTER_RESPONSE:
                    # print(request)
                    continue

                case MessageType.HEARTBEAT:
                    response = {
                        "type": MessageType.HEARTBEAT_RESPONSE,
                        "address": self.node_socket.IDENTITY.decode('utf-8')
                    }
                    self.node_socket.send_json(response)
                    continue

    def send_push_message(self, sender_address, receiver_address, message):
        """sender_address comes already encoded"""
        print(f"Sending push message from {sender_address} to {receiver_address}: {message}")
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
        # TODO should the condition be the length of the most common response?
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

                self.send_push_message(self.node_socket.IDENTITY, node,
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
                    request_to_replicas = build_put_request(key, value, quorum_id)


                    result = ([self.dynamo_node.write_data(key, ShoppingList.from_dict(json.loads(value)))] +
                              self.send_put_request_to_other_nodes(request_to_replicas, task['replicas'], 5, 1,
                                                                   W_QUORUM))

                    print(result)
                    quorum_size = min(W_QUORUM, len(set(task['replicas'])) + 1)
                    if len(result) < quorum_size:
                        result = False
                    else:
                        result = True
                    print("Result  after quorum consensus: ", result)

                    response_to_router = build_quorum_put_response(quorum_id, result)
                    self.node_socket.send_json(response_to_router)

                case MessageType.COORDINATE_GET:
                    quorum_id = task["quorum_id"]
                    key = task["key"]
                    request_to_replicas = build_get_request(key, quorum_id)

                    result = ([self.dynamo_node.read_data(key)] +
                              self.send_get_request_to_other_nodes(request_to_replicas, task['replicas'], 5, 1,
                                                                   R_QUORUM))

                    print(result)
                    quorum_size = min(R_QUORUM, len(set(task['replicas'])) + 1)
                    if len(result) < quorum_size:
                        result = False
                    else:
                        shopping_lists = [ShoppingList.from_dict(json.loads(shopping_list)) for shopping_list in result]
                        merged_shopping_list = self.dynamo_node.merge_shopping_lists(shopping_lists)
                        result = json.dumps(merged_shopping_list, default=lambda x: x.__dict__)
                    print("Result  after quorum consensus: ", result)

                    response_to_router = build_quorum_get_response(quorum_id, result)
                    self.node_socket.send_json(response_to_router)

                case MessageType.COORDINATE_DELETE:
                    quorum_id = task["quorum_id"]
                    key = task["key"]
                    request_to_replicas = build_delete_request(key, quorum_id)

                    result = ([self.dynamo_node.delete_data(key)] +
                              self.send_delete_request_to_other_nodes(request_to_replicas, task['replicas'], 5, 1,
                                                                      R_QUORUM))

                    print(result)
                    quorum_size = min(R_QUORUM, len(set(task['replicas'])) + 1)
                    if len(result) < quorum_size:
                        result = False
                    else:
                        result = True

                    response_to_router = build_quorum_delete_response(quorum_id, result)
                    self.node_socket.send_json(response_to_router)

    def handle_request_response(self, request_type_quorums_state, json_request):
        request_id = json_request['quorum_id']
        if request_id in request_type_quorums_state:
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
            value = self.dynamo_node.read_data(key)
        elif message_type == MessageType.DELETE:
            value = self.dynamo_node.delete_data(key)

        response = {
            "type": message_type + "_RESPONSE",
            "key": key,
            "value": value,
            "address": self.node_socket.IDENTITY.decode('utf-8'),
            "quorum_id": json_request['quorum_id']
        }
        self.send_push_message(self.node_socket.IDENTITY, sender_identity.decode('utf-8'), json.dumps(response))

    def listen_for_nodes(self):
        print("listening for nodes")
        """Messages from other nodes, get, put, delete received from a coordinator or the coordinator itself receives _responses"""
        i = 0
        node_address = self.reply_socket.IDENTITY
        while True:
            # if i > 5:
            #     break
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
