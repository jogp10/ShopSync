from flask import Flask, abort, jsonify, request
from flask_restx import Resource, Api, fields
from flask_cors import CORS
import socket
import atexit

app = Flask(__name__)
api = Api(app)
CORS(app) # Enable cors for all routes

# Replace these import statements with your actual class definitions
from client import Client, show_available_lists, ShoppingListStorageError
from utils import ROUTER_ADDRESS, ROUTER_BACKUP_ADDRESS

PORT_RANGE = range(5000, 5050)  # Adjust the range as needed
def find_available_port():
    for port in PORT_RANGE:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(('localhost', port)) != 0:
                return port
    raise Exception("No available ports in the specified range.")


# Initialize your client
server_addresses = [ROUTER_ADDRESS, ROUTER_BACKUP_ADDRESS]
client_address = f"tcp://localhost:{find_available_port()}"
client = Client(server_addresses, client_address)
client.get_database_data()

#ensure that at exit always do a local save
atexit.register(client.save_database_data)

# Define a namespace for your API
shopping_list_ns = api.namespace('shopping_list', description='Shopping List Operations')

# Define a model for the input data
create_shopping_list_model = api.model('CreateShoppingListModel', {
    'shopping_list_name': fields.String(required=True, description='Name of the shopping list'),
    'items': fields.List(fields.Raw(example=["item1", 5]), description='List of items in the shopping list')
})

sync_shopping_list_model = api.model('SyncShoppingListModel', {
    'shopping_list_uuid': fields.String(required=True, description='Uuid of the shopping list')
})

store_shopping_list_model = api.model('StoreShoppingListModel', {
    'shopping_list_uuid': fields.String(required=True, description='Uuid of the shopping list')
})

delete_shopping_list_model = api.model('DeleteShoppingListModel', {
    'shopping_list_uuid': fields.String(required=True, description='Uuid of the shopping list')
})

delete_item_model = api.model('DeleteItemModel', {
    'shopping_list_uuid': fields.String(required=True, description='Uuid of the shopping list'),
    'item_name': fields.String(required=True, description='Name of the item')
})

add_item_model = api.model('AddItemModel', {
    'shopping_list_uuid': fields.String(required=True, description='Uuid of the shopping list'),
    'item_name': fields.String(required=True, description='Name of the item'),
    'item_quantity': fields.Integer(required=False, description='Quantity of the item', default=1)
})

change_item_quantity_model = api.model('ChangeItemQuantityModel', {
    'shopping_list_uuid': fields.String(required=True, description='Uuid of the shopping list'),
    'item_name': fields.String(required=True, description='Name of the item'),
    'item_quantity': fields.Integer(required=True, description='Quantity of the item')
})

@shopping_list_ns.route('/create')
class CreateShoppingList(Resource):
    @api.doc('create_shopping_list')
    @api.expect(create_shopping_list_model)
    def post(self):
        data = request.json
        name = data.get('shopping_list_name')
        items = [(item[0], int(item[1])) for item in data.get('items', [])]
        print(f"Received request to create shopping list with name {name} and items {items}")
        shopping_list = client.create_shopping_list(name, items)
        print(f"Created shopping list {shopping_list.serialize()}")
        return jsonify({'message': 'Shopping list created successfully', 'shopping_list': shopping_list.serialize()})

@shopping_list_ns.route('/delete')
class DeleteShoppingList(Resource):
    @api.doc('delete_shopping_list')
    @api.expect(delete_shopping_list_model)
    def post(self):
        data = request.json
        uuid = data.get('shopping_list_uuid')
        result = client.delete_shopping_list(uuid)
        return jsonify({'message': result})

@shopping_list_ns.route('/')
class RetrieveMyShoppingLists(Resource):
    @api.doc('retrieve_my_shopping_lists')
    def get(self):
        shopping_lists = client.shopping_lists
        serialized_lists = [shopping_list.serialize() for shopping_list in shopping_lists]
        return jsonify(serialized_lists)

@shopping_list_ns.route('/<string:shopping_list_uuid>')
class RetrieveItemsOfShoppingList(Resource):
    @api.doc('retrieve_items_of_shopping_list')
    def get(self, shopping_list_uuid):
        shopping_list = next(filter(lambda x: x.id == shopping_list_uuid, client.shopping_lists), None)
        return shopping_list.serialize()
    
@shopping_list_ns.route('/item/add')
class AddItemToShoppingList(Resource):
    @api.doc('add_item_to_shopping_list')
    @api.expect(add_item_model)
    def post(self):
        data = request.json
        uuid = data.get('shopping_list_uuid')
        item = (data.get('item_name'), int(data.get('item_quantity', 1)))
        sl = next(filter(lambda x: x.id == uuid, client.shopping_lists), None)
        # Check if item does not exist in shopping list
        if sl.items.value(item[0]) == 0:
            item = (item[0], item[1] - sl.items.value(item[0]))
            result = client.change_item_quantity(uuid, item)
        else:
            result = client.add_item_to_shopping_list(uuid, item)
        return result.serialize()
    
@shopping_list_ns.route('/item/delete')
class DeleteItemFromShoppingList(Resource):
    @api.doc('delete_item_from_shopping_list')
    @api.expect(delete_item_model)
    def post(self):
        data = request.json
        uuid = data.get('shopping_list_uuid')
        item = (data.get('item_name'), 'default_item')
        result = client.delete_item_from_shopping_list(uuid, item)
        return result.serialize()

@shopping_list_ns.route('/item/change')
class ChangeItemQuantityInShoppingList(Resource):
    @api.doc('change_item_quantity_in_shopping_list')
    @api.expect(change_item_quantity_model)
    def post(self):
        data = request.json
        uuid = data.get('shopping_list_uuid')
        item = (data.get('item_name', 'default_item'), int(data.get('item_quantity', 0)))
        sl = next(filter(lambda x: x.id == uuid, client.shopping_lists), None)
        item = (item[0], item[1] - sl.items.value(item[0]))
        result = client.change_item_quantity(uuid, item)
        return result.serialize()
    
@shopping_list_ns.route('/sync')
class SyncShoppingList(Resource):
    @api.doc('sync_shopping_list')
    @api.expect(store_shopping_list_model)
    def post(self):
        data = request.json
        uuid = data.get('shopping_list_uuid')
        
        try:
            result = client.store_shopping_list_online(uuid)
            return jsonify({'message': result})
        except ShoppingListStorageError as e:
            abort(503, description="Failed to connect to the server. Service temporarily unavailable.")

    
@shopping_list_ns.route('/sync_all')
class SyncAllShoppingLists(Resource):
    @api.doc('sync_all_shopping_lists')
    def post(self):
        try:
            result = client.store_shopping_lists_online()
            return jsonify({'message': result})
        except ShoppingListStorageError as e:
            abort(503, description="Failed to connect to the server. Service temporarily unavailable.")
    
@shopping_list_ns.route('/load')
class LoadShoppingList(Resource):
    @api.doc('load_shopping_list')
    @api.expect(sync_shopping_list_model)
    def post(self):
        data = request.json
        uuid = data.get('shopping_list_uuid')

        try:
            result = client.fetch_shopping_list(uuid)
            return result.serialize()
        except ShoppingListStorageError as e:
            abort(503, description="Failed to connect to the server. Service temporarily unavailable.")
    
@shopping_list_ns.route('/load_all')
class LoadAllShoppingLists(Resource):
    @api.doc('load_all_shopping_lists')
    def post(self):
        try:
            result = client.fetch_shopping_lists()
            serialized_lists = [shopping_list.serialize() for shopping_list in result]
            return jsonify(serialized_lists)
        except ShoppingListStorageError as e:
            abort(503, description="Failed to connect to the server. Service temporarily unavailable.")

# Implement other API endpoints similarly...

if __name__ == '__main__':
    port = find_available_port()
    app.run(port=port, debug=True, use_reloader=True)
