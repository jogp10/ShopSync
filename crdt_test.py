from collections import defaultdict
from typing import Dict, Tuple, Union


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

