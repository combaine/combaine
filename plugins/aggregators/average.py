#!/usr/bin/env python
import re

import msgpack

from cocaine.worker import Worker
from cocaine.logging import Logger
from cocaine.services import Service

Log = Logger()


TABLEREGEX = re.compile("%TABLENAME%")
TIMEREGEX = re.compile("TIME\s*=\s*%%")


# class AverageAggregator(object):

#     def __init__(self, **config):
#         self.logger = Logger()
#         super(AverageAggregator, self).__init__()
#         self.query = config['query']
#         self.name = config['name']
#         self._is_rps = config.get("rps", "YES")

#     def aggregate(self, timeperiod):
#         normalize = (timeperiod[1] - timeperiod[0]) if self._is_rps == "YES" else 1

#         def format_me(i):
#             try:
#                 ret = float(i[0][0])/normalize
#                 self.logger.debug("Recalculate to rps: %d/%d = %f" % (i[0][0],
#                                                                       normalize, ret))
#             except Exception:
#                 # May be invalid format - so drop it
#                 return 0  # Special for vyacheslav
#             else:
#                 return ret
#         db = self.dg
#         self.query = self.table_regex.sub(db.tablename, self.query)
#         self.query = self.time_regex.sub("1=1", self.query)  # For backward compability
#         l = (format_me(db.perfomCustomQuery(self.query)), timeperiod[1])
#         self.logger.debug("Result of %s aggreagtion: %s" % (self.name, l))
#         return self.name,  self._pack(l)

#     def _pack(self, data):
#         res = {'time': data[1], 'res': data[0]}
#         return res

#     def _unpack(self, data):
#         """
#         Expected:
#         [ [ {"time" : 12231432, "res" : 13}, ....],
#           [ ..... ],
#         ]
#         """
#         subgroups_count = len(data)
#         data_dict = dict()
#         for group_num, group in enumerate(data):  # iter over subgroups
#             for item in (k for k in group if k is not None):
#                 try:
#                     if data_dict.get(item['time']) is None:
#                         data_dict[item['time']] = list()
#                         [data_dict[item['time']].append(list()) for i in xrange(0, subgroups_count)]
#                     data_dict[item['time']][group_num].append(item['res'])
#                 except Exception:
#                     self.logger.warning("Unexpected format: %s" % str(item))
#         data_sec = data_dict.popitem()
#         return data_sec

#     def aggregate_group(self, data):
#         sec, value = self._unpack(data)
#         per_subgroup_count = list()
#         for subgroup in value:
#             per_subgroup_count.append((sum(subgroup)))
#         group_summ = sum(per_subgroup_count)
#         per_subgroup_count.append(group_summ)
#         self.logger.debug("%s: %s" % (self.name, per_subgroup_count))
#         yield {sec: per_subgroup_count}


def aggregate_host(request, response):
    raw = yield request.read()
    cfg, dgcfg, token, prtime, currtime = msgpack.unpackb(raw)
    Log.info(str(cfg))
    dg = Service(dgcfg['type'])
    #yield dg.connect()
    q = TABLEREGEX.sub(token, cfg['query'])
    q = TIMEREGEX.sub("1=1", q)
    Log.info("QUERY: %s" % q)
    res = yield dg.enqueue("query",
                           msgpack.packb((dgcfg,
                                          token,
                                          q)))

    try:
        ret = float(res[0][0])   # SELECT COUNT(*)
        if cfg.get('rps', '') == "YES":
            Log.info("Recalculate to rps")
            ret = ret / (currtime - prtime)
    except Exception:
        ret = 0
    Log.info(str(ret))
    response.write(msgpack.packb(ret))
    response.close()


def aggregate_group(request, response):
    raw = yield request.read()
    inc = msgpack.unpackb(raw)
    cfg, data = inc
    res = sum(map(msgpack.unpackb, data))
    Log.info("Receive result %s" % res)
    response.write(res) 
    response.close()


if __name__ == '__main__':
    W = Worker()
    W.run({"aggregate_host": aggregate_host,
           "aggregate_group": aggregate_group})
