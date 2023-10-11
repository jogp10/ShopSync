import os
import boto3
import json
from uuid import uuid4

# Initialize AWS DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='eu-north-1', aws_access_key_id='your_access_key', aws_secret_access_key='your_secret_key')
table_name = 'shopping-list'
table = dynamodb.Table(table_name)

# Function to create a new shopping list
def create_shopping_list(name):
    list_id = str(uuid4())
    shopping_list = {
        'id': list_id,
        'name': name,
        'items': [],
    }
    with open(f'lists/{list_id}.json', 'w') as f:
        json.dump(shopping_list, f)
    return shopping_list

# Function to add an item to a shopping list
def add_item(list_id, item):
    with open(f'lists/{list_id}.json', 'r') as f:
        shopping_list = json.load(f)
    shopping_list['items'].append(item)
    with open(f'lists/{list_id}.json', 'w') as f:
        json.dump(shopping_list, f)
    return shopping_list

# Function to sync a shopping list with DynamoDB
def sync_with_dynamodb(list_id):
    with open(f'lists/{list_id}.json', 'r') as f:
        shopping_list = json.load(f)
    table.put_item(Item=shopping_list)

# Function to list all available shopping lists
def list_shopping_lists():
    shopping_lists = [filename.split('.')[0] for filename in os.listdir('lists/') if filename.endswith('.json')]
    return shopping_lists

# Function to get the name of a shopping list
def get_list_name(list_id):
    with open(f'lists/{list_id}.json', 'r') as f:
        shopping_list = json.load(f)
    return shopping_list['name']

# Main function
if __name__ == "__main__":
    while True:
        print("1. Create Shopping List")
        print("2. Add Item to Shopping List")
        print("3. List Available Shopping Lists")
        print("4. Sync Shopping List to Cloud (DynamoDB)")
        choice = input("Enter your choice: ")

        if choice == '1':
            name = input("Enter the name of the shopping list: ")
            shopping_list = create_shopping_list(name)
            print(f'Shopping list created: {shopping_list}')

        elif choice == '2':
            available_lists = list_shopping_lists()
            print("Available Shopping Lists:")
            for idx, list_id in enumerate(available_lists):
                list_name = get_list_name(list_id)
                print(f"{idx + 1}. {list_name} (ID: {list_id})")

            list_index = int(input("Enter the number of the shopping list to which you want to add an item: ")) - 1
            list_id = available_lists[list_index]
            item = input("Enter the item to add: ")
            updated_list = add_item(list_id, item)
            print(f'Shopping list updated: {updated_list}')

        elif choice == '3':
            available_lists = list_shopping_lists()
            print("Available Shopping Lists:")
            for idx, list_id in enumerate(available_lists):
                list_name = get_list_name(list_id)
                print(f"{idx + 1}. {list_name} (ID: {list_id})")

        elif choice == '4':
            list_id = input("Enter the ID of the shopping list: ")
            sync_with_dynamodb(list_id)
            print(f'Shopping list synced to DynamoDB.')

        else:
            break
