# Start of client.py

import json
import os
import sys
from uuid import uuid4
import json
import sqlite3

import zmq

from shopping_list import ShoppingList
from crdt import ShoppingListCRDT
from utils import ROUTER_ADDRESS, MessageType


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


    #Functions for database handling
    def create_database_and_table(self):
        transformed_username = self.username.replace(' ', '_').replace(":", "_").replace("/", "-")
        db_folder = f"db/{transformed_username}/"

        if not os.path.exists(db_folder):
            os.makedirs(db_folder)
        
        conn = sqlite3.connect(os.path.join(db_folder, "client.db"))
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
        transformed_username = self.username.replace(' ', '_').replace(":", "_").replace("/", "-")
        db_folder = f"db/{transformed_username}/"

        if not os.path.exists(db_folder):
            self.create_database_and_table()
            return

        conn = sqlite3.connect(os.path.join(db_folder, "client.db"))
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
            self.shopping_lists.append(ShoppingList.from_dict(shopping_list))
    
    def save_database_data(self):
        transformed_username = transformed_username = self.username.replace(' ', '_').replace(":", "_").replace("/", "-")
        conn = sqlite3.connect(os.path.join("db", transformed_username, "client.db"))
        cursor = conn.cursor()

        # Delete all rows from the shopping_list table
        cursor.execute("DELETE FROM shopping_list")

        for shopping_list in self.shopping_lists:
            shopping_list_dict = shopping_list.to_dict()
            items_json = shopping_list_dict["items"].to_json_string()
    
            cursor.execute("INSERT INTO shopping_list (id, name, items) VALUES (?, ?, ?)",
                   (shopping_list_dict["id"], shopping_list_dict["name"], items_json))
        
        conn.commit()
        conn.close()

    def store_shopping_list_online(self, shopping_list: ShoppingList):
        request = {
            "type": MessageType.PUT,
            "key": shopping_list.id,
            "value": json.dumps(shopping_list, default=lambda x: x.__dict__)
        }
        self.socket.send_json(request, zmq.NOBLOCK)
        response = self.socket.recv_json()
        print(response)

    def fetch_shopping_list(self, list_id):
        request = {
            "type": MessageType.GET, # :()
            "key": list_id
        }

        self.socket.send_json(request, zmq.NOBLOCK)
        response = self.socket.recv_json()
        print(response)

         # TODO: USE THIS TO UPDATE THE LOCAL SHOPPING LIST

    def delete_shopping_list(self, list_id):
        request = {
            "type": MessageType.DELETE,
            "key": list_id
        }
        self.socket.send_json(request, zmq.NOBLOCK)
        response = self.socket.recv_json()
        print(response)




def ask_for_items():
    items = []
    while True:
        name = input("Enter an item (or press Enter to finish): ").strip()
        if name:
            quantity = get_int_from_user("Enter the quantity: ")
            items.append((name, quantity))
        else:
            break
    return items


def show_available_lists(client: Client):
    print("Available Shopping Lists:")
    if client.shopping_lists.__len__() == 0:
        print("No shopping lists available!")
        return
    for idx, shopping_list in enumerate(client.shopping_lists):
        print(f"{idx + 1}. {shopping_list.name} (ID: {shopping_list.id})")
    return (1, client.shopping_lists.__len__())

def get_int_from_user(prompt: str, min: int = -9999, max: int = 9999):
    while True:
        try:
            value = int(input(prompt))
            if value < min or value > max:
                print(f"Please enter a number between {min} and {max}")
                continue
            return value
        except ValueError:
            print("Please enter a number")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python client.py <port>")
        sys.exit(1)
    

    port = sys.argv[1]  # the identity can be anything, the port is not being used by the dealer socket
    client_address = f"tcp://localhost:{port}"

    server_address = ROUTER_ADDRESS
    client = Client(server_address, client_address)
    
    #client.load_local_shopping_lists()
    client.get_database_data()


    while True:
        print("1. Create Shopping List")
        print("2. Add Item to Shopping List")
        print("3. List Available Shopping Lists")
        print("4. Change Item Quantity")
        print("5. Print Shopping List")
        print("6. Sync Shopping List to Cloud")

        # TODO opção que vá buscar uma lista ao servidor por id

        print("7. Load Shopping List from Cloud")
        print("8. Delete Shopping List Permanently")

        print("\n9. Quit and store shopping lists")
        print("0. Quit")
        choice = input("Enter your choice: ").strip().lower()

        if choice == '1':
            name = input("Enter the name of the shopping list: ")
            items = ask_for_items()
            shopping_list = client.create_shopping_list(name, items)
            print(f'Shopping list created: {shopping_list.name}')

        elif choice == '2':
            (min, max) = show_available_lists(client)

            list_index = get_int_from_user("Enter the number of the shopping list to which you want to add an item: ", min, max) - 1
        
            shopping_list: ShoppingList = client.shopping_lists[list_index]
                    
            while True:
                item = input("Enter the item to add: ")

                if shopping_list.item_exists(item):
                    print("Item already exists!")
                    continue
                break

            quantity = get_int_from_user("Enter the quantity: ")

            shopping_list.add_item((item, quantity), client.username)
            print(f'Shopping list updated')

        elif choice == '3':
            show_available_lists(client)

        elif choice == '4':
            (min, max) = show_available_lists(client)


            list_index = get_int_from_user("Enter the number of the shopping list to which you want to change an item: ", min, max) - 1

            shopping_list: ShoppingList = client.shopping_lists[list_index]
            shopping_list.print_items()

            if shopping_list.get_number_of_items() == 0:
                print("No items available!")
                continue

            while True:
                item = input("Enter the item to change: ")
                #Check if item exists
                if not shopping_list.item_exists(item):
                    print("Item does not exist!")
                    continue
                break
            
            quantity = get_int_from_user("Enter the increase or decrease value: ")
            shopping_list.change_item_quantity((item, quantity), client.username)
        
        elif choice == '5':
            (min, max) = show_available_lists(client)

            list_index = get_int_from_user("Enter the number of the shopping list to print: ", min, max) - 1
            shopping_list: ShoppingList = client.shopping_lists[list_index]
            shopping_list.print_items()

        elif choice == '6':
            (min, max) = show_available_lists(client)

            list_index = get_int_from_user("Enter the number of the shopping list to sync to the cloud: ", min, max) - 1
            shopping_list: ShoppingList = client.shopping_lists[list_index]
            client.store_shopping_list_online(shopping_list)

        elif choice == '7':
            list_id = input("Enter the id of the shopping list to load from the cloud: ")
            client.fetch_shopping_list(list_id)

            # uou can try 'lista1'

        elif choice == '8':
            (min, max) = show_available_lists(client)

            list_index = get_int_from_user("Enter the number of the shopping list to delete permanently: ", min, max) - 1
            shopping_list: ShoppingList = client.shopping_lists[list_index]
            client.shopping_lists.remove(shopping_list)

            # TODO: delete from cloud, CHECK IF WORKS
            client.delete_shopping_list(shopping_list.id)

            print(f'Shopping list deleted: {shopping_list.name}')

        elif choice == '9':
            # Store all shopping lists to cloud
            for shopping_list in client.shopping_lists:
                print("Stroring shopping list " + shopping_list.name + " to cloud...")
                client.store_shopping_list_online(shopping_list)

            print("Goodbye!")
            break

        elif choice == '0':
            print("Goodbye!")
            break

        else:
            print("Invalid choice!")

        #Ask the user to press 'c' to continue to the main menu
        input("Press any key to continue to the main menu: ")

    client.save_database_data()
