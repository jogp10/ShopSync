from flask import Flask, jsonify, request
from flask_restx import Resource, Api, fields
import socket
import atexit

app = Flask(__name__)
api = Api(app)

# Replace these import statements with your actual class definitions
from client import Client, show_available_lists
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
shopping_list_model = api.model('ShoppingListInput', {
    'name': fields.String(required=True, description='Name of the shopping list'),
    'items': fields.List(fields.Raw(example=["item1", 5]), description='List of items in the shopping list')
})

@shopping_list_ns.route('/create')
class CreateShoppingList(Resource):
    @api.doc('create_shopping_list')
    @api.expect(api.model('ShoppingListInput', {
        'name': fields.String(required=True, description='Name of the shopping list'),
        'items': fields.List(fields.Raw(example=["item1", 5]), description='List of items in the shopping list')
    }))
    def post(self):
        data = request.json
        name = data.get('name')
        items = [(item[0], int(item[1])) for item in data.get('items', [])]
        print(f"Received request to create shopping list with name {name} and items {items}")
        shopping_list = client.create_shopping_list(name, items)
        return jsonify({'message': 'Shopping list created successfully', 'shopping_list': shopping_list.serialize()})
    
@shopping_list_ns.route('/add_item')
class AddItemToShoppingList(Resource):
    @api.doc('add_item_to_shopping_list')
    @api.expect(shopping_list_model)
    def post(self):
        data = request.json
        name = data.get('name')
        item = (data.get('items', [])[0], int(data.get('items', [])[1]))
        result = client.add_item_to_shopping_list(name, item)
        return jsonify({'message': result})
    
@shopping_list_ns.route('/list')
class RetrieveMyShoppingLists(Resource):
    @api.doc('retrieve_my_shopping_lists')
    def get(self):
        shopping_lists = show_available_lists(client)
        serialized_lists = [shopping_list.serialize() for shopping_list in shopping_lists]
        return jsonify({'shopping_lists': serialized_lists})

@shopping_list_ns.route('/<string:shopping_list_name>')
class RetrieveItemsOfShoppingList(Resource):
    @api.doc('retrieve_items_of_shopping_list')
    @api.expect({'shopping_list_name': fields.String(required=True, description='Name of the shopping list')})
    def get(self, shopping_list_name):
        items = client.shopping_lists(shopping_list_name)
        return jsonify({'items': items})
    
@shopping_list_ns.route('/delete')
class DeleteShoppingList(Resource):
    @api.doc('delete_shopping_list')
    @api.expect({'shopping_list_name': fields.String(required=True, description='Name of the shopping list')})
    def post(self):
        data = request.json
        name = data.get('name')
        result = client.delete_shopping_list(name)
        return jsonify({'message': result})
    
@shopping_list_ns.route('/delete_item')
class DeleteItemFromShoppingList(Resource):
    @api.doc('delete_item_from_shopping_list')
    @api.expect({'shopping_list_name': fields.String(required=True, description='Name of the shopping list')})
    def post(self):
        data = request.json
        name = data.get('name')
        item = (data.get('items', [])[0], int(data.get('items', [])[1]))
        result = client.delete_item_from_shopping_list(name, item)
        return jsonify({'message': result})
    
@shopping_list_ns.route('/sync')
class SyncShoppingList(Resource):
    @api.doc('sync_shopping_list')
    @api.expect({'shopping_list_name': fields.String(required=True, description='Name of the shopping list')})
    def post(self):
        data = request.json
        name = data.get('name')
        result = client.sync_shopping_list(name)
        return jsonify({'message': result})
    
@shopping_list_ns.route('/sync_all')
class SyncAllShoppingLists(Resource):
    @api.doc('sync_all_shopping_lists')
    def post(self):
        result = client.sync_all_shopping_lists()
        return jsonify({'message': result})
    
@shopping_list_ns.route('/load')
class LoadShoppingList(Resource):
    @api.doc('load_shopping_list')
    @api.expect({'shopping_list_name': fields.String(required=True, description='Name of the shopping list')})
    def post(self):
        data = request.json
        name = data.get('name')
        result = client.load_shopping_list(name)
        return jsonify({'message': result})
    
@shopping_list_ns.route('/load_all')
class LoadAllShoppingLists(Resource):
    @api.doc('load_all_shopping_lists')
    def post(self):
        result = client.load_all_shopping_lists()
        return jsonify({'message': result})


# Implement other API endpoints similarly...

if __name__ == '__main__':
    port = find_available_port()
    app.run(port=port, debug=True, use_reloader=True)
