import json
import sys

import zmq

from crdt import ShoppingListCRDT
from hash_ring import HashRing
from utils import MessageType, get_request_type, build_register_request


class DynamoNode:
    def __init__(self, name, data=ShoppingListCRDT.zero()):
        #potentially receive previous data stored in memory
        self.name = name
        self.data: ShoppingListCRDT = data

    def increment_item(self, key, value):
        if value >= 0:
            self.data = self.data.inc(key, self.name, value)
        else:
            self.data = self.data.dec(key, self.name, -value)
        return True  # False if errors?

    def get_data(self, key):
        return self.data.value(key)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python node.py <port>")
        sys.exit(1)

    port = sys.argv[1]
    node_address = f"tcp://localhost:{port}"

    print(f"Node address: {node_address}")

    router_address = "tcp://localhost:5554"  # todo change of more routers

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
                    "value": dynamo_node.get_data(key),
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
                    "value": dynamo_node.increment_item(key, value),
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

        # request = socket.recv( zmq.NOBLOCK
        # Receive the response from the appropriate node using the ZeroMQ socket for that node.

        # Send the response to the client.

        node_socket.send(json.dumps({key: "bar"}).encode('utf-8'))