from collections import defaultdict
from functools import reduce
from typing import Dict, Tuple, Union

from typing import Dict, Union, Optional, Any

ReplicaId = str
Ord = Union[int, float]

Lt = -1
Eq = 0
Gt = 1
Cc = 2


def upsert(k, v, fn, my_map):
    if k not in my_map:
        my_map[k] = v
    else:
        my_map[k] = fn(my_map[k])
    return my_map


def tup2map(kv):
    return {kv[0]: kv[1]}


def merge_option(merge, a, b):
    if a is not None and b is not None:
        return merge(a, b)
    elif a is not None:
        return a
    elif b is not None:
        return b
    else:
        return None


def get_or_else(k, v, my_map):
    return my_map.get(k, v)


VTime = Dict[ReplicaId, int]


class IConvergent:
    def merge(self, a, b):
        pass


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


class MergeOption(IConvergent):
    def __init__(self, nested_merge):
        self.nested_merge = nested_merge

    def merge(self, a, b):
        return merge_option(self.nested_merge, a, b)


MClock = Dict[ReplicaId, VTime]


class MVersion:
    zero = {}

    @staticmethod
    def merge(replica, vtime, clock):
        return upsert(replica, vtime, lambda x: Version.merge(x, vtime), clock.copy())

    @staticmethod
    def min(clock):
        return reduce(Version.min, clock.values(), {})

    @staticmethod
    def max(clock):
        return reduce(Version.max, clock.values(), {})


class Option:
    @staticmethod
    def merge(nested_merge, a, b):
        if a is None and b is None:
            return None
        elif b is None and a is not None:
            return a
        elif a is None and b is not None:
            return b
        else:
            return nested_merge.merge(a, b)



class GCounter:
    def __init__(self, counter_map):
        self.counter_map = counter_map

    @staticmethod
    def zero():
        return GCounter({})

    # @property
    def value(self):
        return sum(self.counter_map.values())

    def inc(self, replica, value):
        updated_counter_map = self.counter_map.copy()
        updated_counter_map[replica] = updated_counter_map.get(replica, 0) + value
        return GCounter(updated_counter_map)

    def merge(self, other_counter):
        merged_counter_map = self.counter_map.copy()
        for replica, value in other_counter.counter_map.items():
            merged_counter_map[replica] = max(merged_counter_map.get(replica, 0), value)
        return GCounter(merged_counter_map)


class Merge(IConvergent):
    def merge(self, a, b):
        return a.merge(b)


class PNCounter:
    def __init__(self, inc, dec):
        self.inc = inc
        self.dec = dec

    @staticmethod
    def zero():
        return PNCounter(GCounter.zero(), GCounter.zero())

    # @property
    def value(self):
        return GCounter.value(self.inc) - GCounter.value(self.dec)

    def inc(self, replica, value):
        return PNCounter(GCounter.inc(replica, value, self.inc), self.dec)

    def dec(self, replica, value):
        return PNCounter(self.inc, GCounter.inc(replica, value, self.dec))

    def merge(self, other_counter):
        merged_inc = GCounter.merge(self.inc, other_counter.inc)
        merged_dec = GCounter.merge(other_counter.dec, self.dec)
        return PNCounter(merged_inc, merged_dec)


class BCounter:
    def __init__(self, counter, others):
        self.counter = counter
        self.others = others

    @staticmethod
    def zero():
        return BCounter(PNCounter.zero(), {})

    def quota(self, replica):
        def fold_fn(acc, p, value):
            src, dst = p
            if src == replica:
                return acc - value
            elif dst == replica:
                return acc + value
            else:
                return acc

        return PNCounter.value(self.counter) + reduce(fold_fn, self.others, 0)

    # @property
    def value(self):
        return PNCounter.value(self.counter)

    def inc(self, replica, value):
        return BCounter(PNCounter.inc(self.counter, replica, value), self.others)

    def dec(self, replica, value):
        quota = self.quota(replica)
        if quota < value:
            return Error(quota)
        else:
            return BCounter(PNCounter.dec(self.counter, replica, value), self.others)

    def move(self, src, dst, value):
        quota = self.quota(src)
        if value > quota:
            return Error(quota)
        else:
            new_others = self.others.copy()
            new_others[src, dst] = new_others.get((src, dst), 0) + value
            return BCounter(self.counter, new_others)

    def merge(self, other):
        merged_counter = PNCounter.merge(self.counter, other.counter)
        merged_others = self.others.copy()
        for key, value in other.others.items():
            merged_others[key] = max(merged_others.get(key, 0), value)
        return BCounter(merged_counter, merged_others)


class BCounterMap:
    def __init__(self, counters):
        self.counters = counters

    def get_or_create(self, id):
        if id not in self.counters:
            self.counters[id] = BCounter.zero()
        return self.counters[id]

    def inc(self, id, replica, value):
        counter = self.get_or_create(id)
        return BCounterMap(self.counters.copy().update({id: counter.inc(replica, value)}))

    def dec(self, id, replica, value):
        counter = self.get_or_create(id)
        result = counter.dec(replica, value)
        if isinstance(result, Error):
            raise ValueError(f"Failed to decrement counter for id {id}: {result.value}")
        else:
            return BCounterMap(self.counters.copy().update({id: result}))

    def move(self, src, dst, value):
        counter = self.get_or_create(src)
        result = counter.move(src, dst, value)
        if isinstance(result, Error):
            raise ValueError(f"Failed to move quota from {src} to {dst}: {result.value}")
        else:
            return BCounterMap(self.counters.copy().update({src: result}))

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
        return BCounterMap(merged_counters)

    def get(self, id):
        return self.counters.get(id, None)


"""
class GCounter:
    def __init__(self, replica_id: str):
        self.counters: Dict[str, int] = {replica_id: 0}

    def inc(self, replica_id: str, value: int):
        self.counters[replica_id] = self.counters.get(replica_id, 0) + value

    def value(self):
        return sum(self.counters.values())

    def merge(self, other):
        merged_counters = defaultdict(int)
        for counter in (self.counters, other.counters):
            for replica_id, value in counter.items():
                merged_counters[replica_id] = max(merged_counters[replica_id], value)
        new_gcounter = GCounter('')
        new_gcounter.counters = dict(merged_counters)
        return new_gcounter


class BCounter:
    def __init__(self, replica_id: str):
        self.pncounter_inc = GCounter(replica_id)
        self.pncounter_dec = GCounter(replica_id)
        self.quota_others: Dict[Tuple[str, str], int] = {}

    def quota(self, replica_id: str):
        return self.pncounter_inc.value() - self.pncounter_dec.value() + self.quota_others.get((replica_id, ''), 0)

    def value(self):
        return self.pncounter_inc.value() - self.pncounter_dec.value()

    def inc(self, replica_id: str, value: int):
        self.pncounter_inc.inc(replica_id, value)

    def dec(self, replica_id: str, value: int):
        q = self.quota(replica_id)
        if q < value:
            return Error(q)
        else:
            self.pncounter_dec.inc(replica_id, value)
            return Ok(self)

    def move(self, src: str, dst: str, value: int):
        q = self.quota(src)
        if value > q:
            return Error(q)
        else:
            if (src, dst) in self.quota_others:
                self.quota_others[(src, dst)] += value
            else:
                self.quota_others[(src, dst)] = value
            return Ok(self)

    def merge(self, other):
        new_pncounter_inc = self.pncounter_inc.merge(other.pncounter_inc)
        new_pncounter_dec = self.pncounter_dec.merge(other.pncounter_dec)

        new_quota_others = {}
        for k, v in self.quota_others.items():
            new_quota_others[k] = max(v, other.quota_others.get(k, 0))

        for k, v in other.quota_others.items():
            if k not in new_quota_others:
                new_quota_others[k] = v

        new_bcounter = BCounter('')
        new_bcounter.pncounter_inc = new_pncounter_inc
        new_bcounter.pncounter_dec = new_pncounter_dec
        new_bcounter.quota_others = new_quota_others

        return new_bcounter

"""
# Helper Result class
class Error(Exception):
    def __init__(self, value):
        self.value = value


class Ok:
    def __init__(self, value):
        self.value = value

# tests
bcounter = BCounter('a')
bcounter.inc('a', 5)
bcounter.dec('a', 2)
bcounter.move('a', 'b', 1)
print(bcounter.value())
print(bcounter.quota('a'))
print(bcounter.quota('b'))
print(bcounter.quota('c'))

bcounter2 = BCounter('b')
bcounter2.inc('b', 3)
bcounter2.dec('b', 1)
bcounter2.move('b', 'a', 1)
print(bcounter2.value())

bcounter3 = bcounter.merge(bcounter2)
print(bcounter3.value())
