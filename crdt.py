from crdt import CRDT

class ShoppingList:
    def __init__(self):
        self.crdt = CRDT()

    def add_item(self, item):
        self.crdt.add(item)

    def remove_item(self, item):
        self.crdt.remove(item)

    def get_items(self):
        return list(self.crdt)

    def clear_list(self):
        self.crdt.clear()