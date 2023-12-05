import json
import sys
import threading
from typing import Dict
import os
import sqlite3
import time

import zmq


from crdt import ShoppingListCRDT, upsert, upsert_list
from shopping_list import ShoppingList
from utils import MessageType, get_request_type, build_register_request, ROUTER_ADDRESS


class DynamoNode:
    def __init__(self, name, data: dict = {}):
        self.name = name
        self.data: dict = data  # a dict k: list id, v: ShoppingList[]
        self.dirty: dict = {} # a dict k: list id, v: True if dirty, False otherwise

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
        return True # False if errors?

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

            return json.dumps(merged_data, indent=2, default=lambda x: x.__dict__)
        
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

    def save_all_database_data(self):
        transformed_name = self.name.replace(' ', '_').replace(":", "_").replace("/", "-")
        db_folder = f"db_server/{transformed_name}/"
        conn = sqlite3.connect(os.path.join(db_folder, "node.db"))
        cursor = conn.cursor()

        # Delete all rows from the shopping_list table
        cursor.execute("DELETE FROM shopping_list")

        for shopping_list in self.data.values():
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


def send_push_message(context, sender_address, receiver_address, message):
    push_socket = context.socket(zmq.PUSH)
    push_socket.connect(receiver_address)
    push_socket.send_multipart([sender_address, message.encode('utf-8')])
    push_socket.close()

def listen_for_nodes(context, pull_socket):
    i = 0
    node_address = pull_socket.IDENTITY
    while True:
        if i > 5:
            break
        identity, message = pull_socket.recv_multipart()
        print(identity)
        print(message)
        print('\n\n')
        # print(message)
        # print(f"Received reply from {address}: {message}")

        address = identity.decode('utf-8')
        print("Received reply from " + address)
        # reply_socket.send_json({0: "OK"})
        push_socket = context.socket(zmq.PUSH)
        push_socket.connect(address)
        push_socket.send_multipart([node_address, json.dumps(build_register_request("ok")).encode('utf-8')])
        push_socket.close()
        i += 1



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python node.py <port>")
        sys.exit(1)

    print("Debugger plz, don't use cached version")

    port = sys.argv[1]
    node_address = f"tcp://localhost:{port}"

    # print(f"Node address: {node_address}")

    router_address = ROUTER_ADDRESS # todo change if more routers

    # Create a new context and socket for the node
    context = zmq.Context()
    node_socket = context.socket(zmq.DEALER)
    node_socket.setsockopt(zmq.IDENTITY, node_address.encode('utf-8'))
    node_socket.connect(router_address)


    # send a first message to the router to register the node
    node_socket.send_json(build_register_request(node_address))

    reply_socket = context.socket(zmq.PULL)
    reply_socket.setsockopt(zmq.IDENTITY, node_address.encode('utf-8'))
    reply_socket.bind(f"tcp://*:{port}")

    # start a thread
    reply_thread = threading.Thread(target=listen_for_nodes, args=(context, reply_socket,))
    reply_thread.start()

    # if port != "6000" send message to 6000 to register
    if port != "6000":
    #     create a push socket
        push_socket = context.socket(zmq.PUSH)
        push_socket.connect("tcp://localhost:6000")
        push_socket.send_multipart([node_address.encode('utf-8'), json.dumps(build_register_request("ok")).encode('utf-8')])
        push_socket.close()
        print("Sent message to 6000")

    # Create a DynamoNode instance for this node
    dynamo_node = DynamoNode(node_address)

    dynamo_node.get_database_data()

    # Start listening for messages
    while True:
        # TODO usar polling ou selector para verificar/escolher se tem mensagens?

        #  receve from either the router or the client, nowaits
        request = node_socket.recv_json()


        print(request)

        # switch case on request type
        match get_request_type(request):

            case MessageType.GET:
                key = request['key']
                response = {
                    "type": MessageType.GET_RESPONSE,
                    "key": key,
                    "value": dynamo_node.read_data(key),
                    "address": node_socket.IDENTITY.decode('utf-8'),
                    "quorum_id": request['quorum_id']
                }
                # time.sleep(1)
                node_socket.send_json(response)

                #SAVE TO DATABASE, CAN BE MADE AFTER SENDING THE RESPONSE TO THE USER, ONLY MADE IF INFORMATION DIFFERENT FROM DATABASE (DIRTY IS TRUE)
                if(dynamo_node.dirty.get(key)):
                    dynamo_node.save_shopping_list_database(key)
                continue

            case MessageType.PUT:
                key = request['key']
                value = request['value']

                response = {
                    "type": MessageType.PUT_RESPONSE,
                    "key": key,
                    "value": dynamo_node.write_data(key, ShoppingList.from_dict(json.loads(value))),
                    "address": node_socket.IDENTITY.decode('utf-8'),
                    "quorum_id": request['quorum_id']
                }
                # time.sleep(1)
                node_socket.send_json(response)
                continue

            case MessageType.DELETE:
                key = request['key']
                response = {
                    "type": MessageType.DELETE_RESPONSE,
                    "key": key,
                    "value": dynamo_node.delete_data(key),
                    "address": node_socket.IDENTITY.decode('utf-8'),
                    "quorum_id": request['quorum_id']
                }
                # time.sleep(1)
                node_socket.send_json(response)


            case MessageType.REGISTER_RESPONSE:
                # print(request)
                continue

            case MessageType.HEARTBEAT:
                response = {
                    "type": MessageType.HEARTBEAT_RESPONSE,
                    "address": node_socket.IDENTITY.decode('utf-8')
                }
                node_socket.send_json(response)
                continue


