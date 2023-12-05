import json
import sys
from typing import Dict
import os
import sqlite3
import atexit

import zmq


from crdt import ShoppingListCRDT, upsert, upsert_list
from shopping_list import ShoppingList
from utils import MessageType, get_request_type, build_register_request, ROUTER_ADDRESS


class DynamoNode:
    def __init__(self, name, data: dict = {}):
        self.name = name
        self.data: dict = data  # a dict k: list id, v: ShoppingList[]
        self.dirty: dict = {} # a dict k: list id, v: True if dirty, False otherwise

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


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python node.py <port>")
        sys.exit(1)

    print("Debugger plz, don't use cached version")

    port = sys.argv[1]
    node_address = f"tcp://localhost:{port}"

    print(f"Node address: {node_address}")

    router_address = ROUTER_ADDRESS # todo change if more routers

    # Create a new context and socket for the node
    context = zmq.Context()
    node_socket = context.socket(zmq.DEALER)
    node_socket.setsockopt(zmq.IDENTITY, node_address.encode('utf-8'))
    node_socket.connect(router_address)


    # send a first message to the router to register the node
    node_socket.send_json(build_register_request(node_address))

    # Create a DynamoNode instance for this node
    dynamo_node = DynamoNode(node_address)

    dynamo_node.get_database_data()

    #ensure that at exit the database is saved
    atexit.register(dynamo_node.save_all_database_data)

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


