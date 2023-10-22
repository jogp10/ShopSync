from uuid import uuid4, UUID

import requests
import hashlib


# class Item:
#     def __init__(self, name: str, quantity: int):
#         self.name = name
#         self.quantity = quantity
#
#     def __repr__(self):
#         return f"Item(name={self.name}, quantity={self.quantity})"
#
#     def set_quantity(self, new_quantity: int):
#         self.quantity = new_quantity
#
#     def change_quantity(self, delta: int):
#         self.quantity += delta

# class ShoppingListManager:
#     def __init__(self, dht_host, dht_port):
#         self.dht_host = dht_host
#         self.dht_port = dht_port
#
#     def get(self, id):
#         key = self._hash_id(id)
#         value = requests.get(f"http://{self.dht_host}:{self.dht_port}/get/{key}").json()
#         if value is None:
#             return None
#         else:
#             return ShoppingList(id, value["items"], value["shared_url"])
#
#     def put(self, shopping_list):
#         key = self._hash_id(shopping_list.id)
#         value = {
#             "items": shopping_list.items,
#             "shared_url": shopping_list.shared_url,
#         }
#         requests.post(f"http://{self.dht_host}:{self.dht_port}/put/{key}", json=value)
#
#     def delete(self, id):
#         key = self._hash_id(id)
#         requests.delete(f"http://{self.dht_host}:{self.dht_port}/delete/{key}")
#
#     def _hash_id(self, id):
#         return hashlib.sha256(id.encode()).hexdigest()
class ShoppingList:
    def __init__(self, id: str, name: str, items: dict[str, int] = None):
        self.id = id  # will probably be used for the url, at least for now
        self.name = name.strip()
        self.items = items

    def __repr__(self):
        return f"ShoppingList(name={self.name}, id={self.id}, items={self.items})"

    def set_items(self, items: dict[str, int]):
        self.items = items

    def add_item(self, item: str | tuple[str, int]):
        if item[0] in [it.name for it in self.items]:
            raise ValueError(f"Item with name {item[0]} already exists in shopping list.")
        # TODO to be changed later??
        elif isinstance(item, tuple):
            self.items[item[0]] = item[1]
        else:
            self.items[item] = 1

    def remove_item(self, item: tuple[str, int] | str):
        if isinstance(item, tuple):
            item = item[0]
        self.items.pop(item)

    def set_item_quantity(self, item: tuple[str, int] | str, new_quantity: int):
        if isinstance(item, tuple):
            item = item[0]
        self.items[item] = new_quantity

    def change_item_quantity(self, item: tuple[str, int] | str, delta: int):
        if isinstance(item, tuple):
            item = item[0]
        self.items[item] += delta

    # convert to a json serializable dict
    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "items": self.items
        }

