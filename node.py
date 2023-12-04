import json
import sys
from typing import Dict

import zmq

from crdt import ShoppingListCRDT, upsert, upsert_list
from shopping_list import ShoppingList
from utils import MessageType, get_request_type, build_register_request, ROUTER_ADDRESS


class DynamoNode:
    def __init__(self, name, data: dict = {}):
        #potentially receive previous data stored in memory
        self.name = name
        self.data: dict = data  # a dict k: list id, v: ShoppingListCRDT[]

        # TODO remove, debug, etc...

        mock_list = ShoppingListCRDT.zero()
        mock_list = mock_list.inc("banana", "a", 1)
        mock_list = mock_list.inc("apple", "a", 2)
        mock_list = mock_list.inc("orange", "a", 3)
        self.data['lista1'] = [mock_list]


    def write_data(self, list_id, data):
        shopping_list = ShoppingList.from_dict(json.loads(data))
        upsert_list(list_id, shopping_list.items, self.data)
        return True # False if errors?

    def read_data(self, list_id):
        if self.data[list_id] is None:
            return None
        else:
            # merge all the data in the list
            merged_data = ShoppingListCRDT.zero()
            for shopping_list_crdt in self.data[list_id]:
                merged_data = merged_data.merge(shopping_list_crdt)

            self.data[list_id] = [merged_data]  # assuming no need to maintain history??
            return json.dumps(merged_data, indent=2, default=lambda x: x.__dict__)



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
                continue

            case MessageType.PUT:
                key = request['key']
                value = request['value']
                response = {
                    "type": MessageType.PUT_RESPONSE,
                    "key": key,
                    "value": dynamo_node.write_data(key, value),
                    "address": node_socket.IDENTITY.decode('utf-8'),
                    "quorum_id": request['quorum_id']
                }
                # time.sleep(1)
                node_socket.send_json(response)
                continue

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

