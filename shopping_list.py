from uuid import uuid4, UUID

import requests
import hashlib

from crdt import ShoppingListCRDT


class ShoppingList:
    """Contrary to the functional style of the CRDT classes, this class is mutable.
    Avoid using the constructor with a list of items and without a replica_id
    """
    def __init__(self, id: str, name: str, items: ShoppingListCRDT | list[tuple[str, int]] = ShoppingListCRDT.zero(), replica_id: str = str(uuid4())):
        self.id = id  # will probably be used for the url, at least for now
        self.name = name.strip()

        if isinstance(items, list):
            self.items = ShoppingListCRDT.zero()
            for item in items:
                self.add_item(item, replica_id)
        else:
            self.items = items

    def __repr__(self):
        return f"ShoppingList(name={self.name}, id={self.id}, items={self.items})"

    def add_item(self, item: tuple[str, int], replica_id: str):
        if item[0] in [item_name for item_name in self.items.counters]:
            raise ValueError(f"Item with name {item[0]} already exists in shopping list.")
        else:
            self.items = self.items.inc(item[0], replica_id, item[1])

    def remove_item(self, item: tuple[str, int] | str):
        if isinstance(item, tuple):
            item = item[0]
        self.items = self.items.delete(item)

    def change_item_quantity(self, item: tuple[str, int], replica_id: str):
        """item is a tuple of the form (item_name, delta)"""
        if item[0] not in [item_name for item_name in self.items.counters]:
            raise ValueError(f"Item with name {item[0]} does not exist in shopping list.")
        elif item[1] > 0:
            self.items = self.items.inc(item[0], replica_id, item[1])
        else:
            self.items = self.items.dec(item[0], replica_id, -item[1])

    # convert to a json serializable dict
    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "items": self.items
        }

    @staticmethod
    def from_dict(d: dict):
        parsed_items = ShoppingListCRDT.from_dict(d['items'])
        return ShoppingList(d['id'], d['name'], parsed_items)
