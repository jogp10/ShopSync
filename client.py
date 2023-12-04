# Start of client.py

import json
import os
import sys
from uuid import uuid4

import zmq

from shopping_list import ShoppingList
from utils import ROUTER_ADDRESS


class Client:
    def __init__(self, server_address, username):
        self._context = zmq.Context()
        self.socket = self._context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, username.encode('utf-8'))
        self.socket.connect(server_address)
        self.username = username
        self.shopping_lists = []

    def get(self, key):
        # Create a request message.

        request = {
            "key": key,
            "type": "get"
        }

        # Send the request to the hash table.
        self.socket.send_json(request, zmq.NOBLOCK)

        # Receive the response from the hash table.
        response = self.socket.recv_json()
        value = response[key]

        # Return the response.

        return value

    def create_shopping_list(self, name: str, items: list[tuple[str, int]] = None):
        id = str(uuid4())
        shopping_list = ShoppingList(id, name, items, self.username)
        self.shopping_lists.append(shopping_list)
        return shopping_list

    def load_local_shopping_lists(self):
        transformed_username = self.username.replace(' ', '_').replace(":", "_").replace("/", "-")
        if not os.path.exists('lists/' + transformed_username):
            os.makedirs('lists/' + transformed_username)
        for filename in os.listdir('lists/' + transformed_username):
            if filename.endswith('.json'):
                with open(f'lists/{transformed_username}/{filename}', 'r') as f:
                    shopping_list = json.load(f)
                self.shopping_lists.append(ShoppingList.from_dict(shopping_list))
                print(f'Loaded shopping list {shopping_list["name"]} from local storage.')
                print(self.shopping_lists[-1])

    def store_shopping_lists_locally(self):
        transformed_username = self.username.replace(' ', '_').replace(":", "_").replace("/", "-")
        if not os.path.exists('lists/' + transformed_username):
            os.makedirs('lists/' + transformed_username)
        for shopping_list in self.shopping_lists:
            with open(f'lists/{transformed_username}/{shopping_list.id}.json', 'w') as f:
                json.dump(shopping_list, indent=2, default=lambda x: x.__dict__, fp=f)

    def store_shopping_list_in_online(self, shopping_list: ShoppingList):
        s_list = shopping_list.to_dict()
        request = {
            "type": 'put',
            "shopping_list": s_list
        }
        self.socket.send_json(request, zmq.NOBLOCK)
        response = self.socket.recv_json()
        print(response)


def ask_for_items():
    items = []
    while True:
        name = input("Enter an item (or press Enter to finish): ").strip()
        if name:
            quantity = int(input("Enter the quantity: "))
            items.append((name, quantity))
        else:
            break
    return items


def show_available_lists(client: Client):
    print("Available Shopping Lists:")
    for idx, shopping_list in enumerate(client.shopping_lists):
        print(f"{idx + 1}. {shopping_list.name} (ID: {shopping_list.id})")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python client.py <port>")
        sys.exit(1)

    port = sys.argv[1]  # the identity can be anything, the port is not being used by the dealer socket
    client_address = f"tcp://localhost:{port}"

    server_address = ROUTER_ADDRESS
    # username = "client 1"
    client = Client(server_address, client_address)
    client.load_local_shopping_lists()

    # Get the value for the key "foo".
    # value = client.get("fooo")

    while True:
        print("1. Create Shopping List")
        print("2. Add Item to Shopping List")
        print("3. List Available Shopping Lists")
        # print("4. Sync Shopping List to Cloud (DynamoDB)")
        print("5. Quit")
        choice = input("Enter your choice: ").strip().lower()

        if choice == '1':
            name = input("Enter the name of the shopping list: ")
            items = ask_for_items()
            shopping_list = client.create_shopping_list(name, items)
            print(f'Shopping list created: {shopping_list.name}')

        elif choice == '2':
            show_available_lists(client)

            list_index = int(input("Enter the number of the shopping list to which you want to add an item: ")) - 1
            shopping_list: ShoppingList = client.shopping_lists[list_index]
            item = input("Enter the item to add: ")
            quantity = int(input("Enter the quantity: "))
            shopping_list.add_item((item, quantity), client.username)
            print(f'Shopping list updated')

        elif choice == '3':
            show_available_lists(client)

        elif choice == '4':
            list_id = input("Enter the ID of the shopping list: ")
            # ....
            print(f'Shopping list synced to DynamoDB.......')

        elif choice == '5':
            print("Goodbye!")
            break

        else:
            print("Invalid choice!")

    client.store_shopping_lists_locally()
    # client.store_shopping_list_in_online(client.shopping_lists[0])
