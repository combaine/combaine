#!/usr/bin/env python

import quant
import random

MAX_COUNT = 1000

DATA = sorted([random.randint(0, 1000) for _ in xrange(0, random.randint(10, MAX_COUNT))])
QUANT = [75, 90, 93, 94, 95, 96, 97, 98, 99]


d = quant.quantile_packer(DATA)
assert(d["count"] == len(DATA))
exp_l = map(int, map(lambda x: float(d["count"]) * x / 100, QUANT))
print exp_l, d["count"]
expected = [DATA[i] for i in exp_l]
print expected
d = quant.merge([d])
res = quant.quants(exp_l, d["data"])
print res
# assert len(res) == len(expected), "WRONG LEN"
assert all(map(lambda x: x[0] == x[1], zip(expected, res)))
