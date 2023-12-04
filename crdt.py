import json
from typing import Dict
from collections import defaultdict
import ast

ReplicaId = str

Lt = -1
Eq = 0
Gt = 1
Cc = 2


class GCounter:
    def __init__(self, counter_map, replica_clock=None):
        self.counter_map = counter_map
        self.replica_clock = defaultdict(int) if replica_clock is None else replica_clock


    @staticmethod
    def zero():
        return GCounter({}, {})

    @staticmethod
    def from_dict(d):
        return GCounter(d['counter_map'], d['replica_clock'])

    def __repr__(self):
        return f"GCounter(value={self.value()}, counter_map={self.counter_map}, replica_clock={self.replica_clock})"

    # @property
    def value(self):
        return sum(self.counter_map.values())

    def inc(self, replica, value):
        updated_counter_map = self.counter_map.copy()
        updated_counter_map[replica] = updated_counter_map.get(replica, 0) + value

        updated_replica_clock = self.replica_clock.copy()
        updated_replica_clock[replica] = updated_replica_clock.get(replica, 0) + 1

        return GCounter(updated_counter_map, updated_replica_clock)

    def compare_clocks(self, clock_a, clock_b):
        keys = set(clock_a.keys()) | set(clock_b.keys())
        prev = Eq

        # ex: clock_a = {'a': 1, 'b': 2, 'c': 1}, clock_b = {'a': 1, 'b': 1, 'c': 2} are concurrent
        # the merge of these two clocks is {'a': 1, 'b': 2, 'c': 2}

        for k in keys:
            va = clock_a.get(k, 0)
            vb = clock_b.get(k, 0)

            if prev == Eq:
                if va > vb:
                    prev = Gt
                elif va < vb:
                    prev = Lt
            elif prev == Lt and va > vb:
                return Cc
            elif prev == Gt and va < vb:
                return Cc

        return prev

    def merge(self, other_counter):
        comparison_result = self.compare_clocks(self.replica_clock, other_counter.replica_clock)

        if comparison_result == Lt:
            merged_replica_clock = other_counter.replica_clock.copy()
        elif comparison_result == Gt:
            merged_replica_clock = self.replica_clock.copy()
        else:
            # Concurrent, resolve conflicts based on the maximum values
            merged_replica_clock = {k: max(self.replica_clock.get(k, 0), other_counter.replica_clock.get(k, 0))
                                    for k in set(self.replica_clock) | set(other_counter.replica_clock)}

        merged_counter_map = self.counter_map.copy()

        # use the merged replica clock to resolve conflicts
        keys = set(self.replica_clock.keys()) | set(other_counter.replica_clock.keys())
        for replica in keys:
            if self.replica_clock.get(replica, 0) == merged_replica_clock.get(replica, -1):
                merged_counter_map[replica] = self.counter_map.get(replica, 0)
            elif other_counter.replica_clock.get(replica, 0) == merged_replica_clock.get(replica, -1):
                merged_counter_map[replica] = other_counter.counter_map.get(replica, 0)
            else:
                print("This should never happen")
                merged_counter_map[replica] = max(self.counter_map.get(replica, 0),
                                                  other_counter.counter_map.get(replica, 0))

        return GCounter(merged_counter_map, merged_replica_clock)


class PNCounter:
    def __init__(self, inc, dec):
        self.inc_counter = inc
        self.dec_counter = dec

    def __repr__(self):
        return f"PNCounter(value={self.value()}, inc={self.inc_counter}, dec={self.dec_counter})"

    @staticmethod
    def zero():
        return PNCounter(GCounter.zero(), GCounter.zero())

    # @property
    def value(self):
        return max(GCounter.value(self.inc_counter) - GCounter.value(self.dec_counter), 0)

    def inc(self, replica, value):
        return PNCounter(GCounter.inc(self.inc_counter, replica, value), self.dec_counter)

    def dec(self, replica, value):
        return PNCounter(self.inc_counter, GCounter.inc(self.dec_counter, replica, value))

    def merge(self, other_counter):
        merged_inc = GCounter.merge(self.inc_counter, other_counter.inc_counter)
        merged_dec = GCounter.merge(other_counter.dec_counter, self.dec_counter)
        return PNCounter(merged_inc, merged_dec)


class ShoppingListCRDT:
    """This is essentially a PNCounterMap"""

    def __repr__(self):
        return f"ShoppingListCRDT(counters={self.counters})"

    @staticmethod
    def zero():
        return ShoppingListCRDT({})

    def __init__(self, counters):
        self.counters: Dict[str, PNCounter] = counters

    def get_or_create(self, id):
        if id not in self.counters:
            self.counters[id] = PNCounter.zero()
        return self.counters[id]

    def inc(self, id, replica, value):
        assert value >= 0
        counter = self.get_or_create(id)
        updated_counters = self.counters.copy()
        updated_counters.update({id: counter.inc(replica, value)})
        return ShoppingListCRDT(updated_counters)

    def dec(self, id, replica, value):
        assert value >= 0
        counter = self.get_or_create(id)
        updated_counters = self.counters.copy()
        updated_counters.update({id: counter.dec(replica, value)})
        return ShoppingListCRDT(updated_counters)

    def merge(self, other):
        merged_counters = {}
        for id, counter in self.counters.items():
            if id in other.counters:
                merged_counters[id] = counter.merge(other.counters[id])
            else:
                merged_counters[id] = counter
        for id, counter in other.counters.items():
            if id not in merged_counters:
                merged_counters[id] = counter
        return ShoppingListCRDT(merged_counters)

    def value(self, key):
        return self.get_or_create(key).value()

    def delete(self, key):
#         decrement by the current value
        counter = self.get_or_create(key)
        updated_counters = self.counters.copy()
        updated_counters.update({key: counter.dec(key, counter.value())})
        return ShoppingListCRDT(updated_counters)

    @staticmethod
    def from_dict(counters):
        counters = {k: PNCounter(GCounter.from_dict(v['inc_counter']), GCounter.from_dict(v['dec_counter'])) for k, v in counters['counters'].items()}
        return ShoppingListCRDT(counters)


# """Things that are not actually used but may serve as inspiration for any tweaks"""

def upsert(k, v, fn, my_map):
    if k not in my_map:
        my_map[k] = v
    else:
        my_map[k] = fn(my_map[k])
    return my_map


VTime = Dict[ReplicaId, int]
MClock = Dict[ReplicaId, VTime]


class Version:
    zero = {}

    @staticmethod
    def inc(r, vv):
        return upsert(r, 1, lambda x: x + 1, vv.copy())

    @staticmethod
    def set(r, ts, vv):
        vv[r] = ts
        return vv

    @staticmethod
    def max(vv1, vv2):
        return {k: vv2[k] if k in vv2 else max(vv1[k], vv2[k]) for k in vv1.keys() | vv2.keys()}

    @staticmethod
    def min(vv1, vv2):
        return {k: vv2[k] if k in vv2 else min(vv1[k], vv2[k]) for k in vv1.keys() | vv2.keys()}

    @staticmethod
    def merge(a, b):
        return Version.max(a, b)

    @staticmethod
    def compare(a, b):
        keys = set(a.keys()) | set(b.keys())
        prev = Eq
        for k in keys:
            va = a.get(k, 0)
            vb = b.get(k, 0)
            if prev == Eq:
                if va > vb:
                    prev = Gt
                elif va < vb:
                    prev = Lt
            elif prev == Lt and va > vb:
                return Cc
            elif prev == Gt and va < vb:
                return Cc
        return prev


if __name__ == '__main__':
    pncounter = PNCounter.zero()
    pncounter = pncounter.inc('a', 5)
    pncounter = pncounter.dec('a', 2)
    print(pncounter.value())

    pncounter2 = PNCounter.zero()
    pncounter2 = pncounter2.inc('b', 3)
    pncounter2 = pncounter2.dec('b', 1)
    pncounter2 = pncounter2.inc('a', 1)
    print(pncounter2.value())

    pncounter3 = pncounter.merge(pncounter2)
    print(pncounter3.value())

    # test the gcounter idempotency
    gcounter = GCounter.zero()
    gcounter = gcounter.inc('a', 5)
    gcounter = gcounter.inc('a', 5)
    print(gcounter.value())
    gcounter2 = GCounter({'a': 12}, {'a': 4})
    gcounter3 = gcounter.merge(gcounter2)
    gcounter3 = gcounter3.merge(gcounter2)
    print(gcounter3.value())
