# from crdt import CRDT
"""
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
        """

from typing import TypeVar, Generic
from datetime import datetime
from collections import defaultdict

#
# # AWORSet implementation
# class AWORSet:
#     def __init__(self):
#         self.replicas = defaultdict(set)
#
#     @staticmethod
#     def zero():
#         return AWORSet()
#
#     def value(self):
#         return set.union(*self.replicas.values())
#
#     def add(self, replica, item):
#         self.replicas[replica].add(item)
#
#     def rem(self, replica, item):
#         if replica in self.replicas:
#             self.replicas[replica].discard(item)
#
#     @staticmethod
#     def merge(a, b):
#         result = AWORSet()
#         for replica, items in a.replicas.items():
#             result.replicas[replica].update(items)
#         for replica, items in b.replicas.items():
#             result.replicas[replica].update(items)
#         return result


####
from typing import TypeVar, Generic
from enum import Enum
from collections import defaultdict


# Ord enumeration
class Ord(Enum):
    Lt = -1
    Eq = 0
    Gt = 1
    Cc = 2


# Version class
class Version:
    def __init__(self, replica, counter):
        self.replica = replica
        self.counter = counter

    @staticmethod
    def zero(replica):
        return Version(replica, 0)

    def inc(self):
        return Version(self.replica, self.counter + 1)

    @staticmethod
    def compare(version1, version2):
        if version1.counter < version2.counter:
            return Ord.Lt
        elif version1.counter > version2.counter:
            return Ord.Gt
        else:
            return Ord.Cc

    @staticmethod
    def merge(version1, version2):
        if version1.replica == version2.replica:
            return version1 if version1.counter > version2.counter else version2
        else:
            raise ValueError("Cannot merge versions from different replicas")


# VTime class
class VTime:
    def __init__(self):
        self.versions = defaultdict(Version)

    @staticmethod
    def zero(replica):
        return VTime()

    def inc(self, replica):
        self.versions[replica] = self.versions[replica].inc()

# AWORSet class
class AWORSet:
    def __init__(self, add, rem):
        self.add = add
        self.rem = rem

    @staticmethod
    def zero():
        return AWORSet(defaultdict(VTime), defaultdict(VTime))

    def value(self):
        rem = self.rem
        add = self.add
        for k, vr in rem.items():
            va = add[k]
            if va and Version.compare(va, vr) == Ord.Lt:
                del add[k]
        return set(add.keys())

    def add(self, replica, element):
        va = self.add[element]
        vr = self.rem[element]
        if va and not vr:
            self.add[element] = va.inc(replica)
        elif not va and vr:
            self.add[element] = Version.zero(replica)
            del self.rem[element]
        elif va and vr:
            self.add[element] = va.inc(replica)
            del self.rem[element]
        else:
            self.add[element] = Version.zero(replica)

    def rem(self, replica, element):
        va = self.add[element]
        vr = self.rem[element]
        if va and not vr:
            self.rem[element] = va.inc(replica)
            del self.add[element]
        elif not va and vr:
            self.rem[element] = vr.inc(replica)
        elif va and vr:
            self.rem[element] = vr.inc(replica)
        else:
            self.rem[element] = Version.zero(replica)

    @staticmethod
    def merge(set1, set2):
        def merge_keys(a, b):
            result = defaultdict(Version)
            for k, vb in b.items():
                va = a[k]
                if va:
                    result[k] = Version.merge(va, vb)
                else:
                    result[k] = vb
            return result

        add1 = merge_keys(set1.add, set2.add)
        rem1 = merge_keys(set1.rem, set2.rem)

        for k, vr in rem1.items():
            va = add1[k]
            if va and Version.compare(va, vr) == Ord.Lt:
                del add1[k]

        for k, va in add1.items():
            vr = rem1[k]
            if vr and Version.compare(va, vr) != Ord.Lt:
                del rem1[k]

        return AWORSet(add1, rem1)


# Example usage:
# Create two AWORSet instances
awor_set1 = AWORSet.zero()
awor_set2 = AWORSet.zero()

# Add some elements to the sets
awor_set1.add("replica1", awor_set1)
awor_set1.add("replica1", awor_set1)
awor_set2.add("replica2", awor_set2)
awor_set2.add("replica2", awor_set2)

# Merge the two AWORSet instances
merged_awor_set = AWORSet.merge(awor_set1, awor_set2)

# Print the merged elements
print(merged_awor_set.value())


class AWORMap:
    def __init__(self, keys, entries):
        self.keys = keys
        self.entries = entries

    @staticmethod
    def zero():
        return AWORMap(AWORSet.zero(), {})

    def value(self):
        return self.entries

    def add(self, replica, key, value):
        self.keys.add(replica, key)
        self.entries[key] = value

    def rem(self, replica, key):
        self.keys.rem(replica, key)
        if key in self.entries:
            del self.entries[key]

    @staticmethod
    def merge(a, b):
        keys = AWORSet.merge(a.keys, b.keys)
        entries = {}
        for key in keys.value():
            value_a = a.entries.get(key)
            value_b = b.entries.get(key)
            if value_a is not None and value_b is not None:
                merged_value = value_a.merge(value_a, value_b)
                entries[key] = merged_value
            elif value_a is not None:
                entries[key] = value_a
            elif value_b is not None:
                entries[key] = value_b
        return AWORMap(keys, entries)


# Example usage:
# Create two AWORMap instances
awor_map1 = AWORMap.zero()
awor_map2 = AWORMap.zero()

set1 = AWORSet()
set1.add("replica1", "item1")

set2 = AWORSet()
set2.add("replica2", "item2")

# Add some entries to the maps
awor_map1.add("replica1", "key1", set1)
awor_map1.add("replica1", "key2", set2)
awor_map2.add("replica2", "key1", AWORSet())
awor_map2.add("replica2", "key3", AWORSet())

# Merge the two AWORMap instances
merged_awor_map = AWORMap.merge(awor_map1, awor_map2)

# Print the merged entries
print(merged_awor_map.value())
