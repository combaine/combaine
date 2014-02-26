#!/usr/bin/env python
import re
import itertools
import collections

import msgpack

from cocaine.worker import Worker
from cocaine.logging import Logger
from cocaine.services import Service

Log = Logger()

TABLEREGEX = re.compile("%TABLENAME%")
TIMEREGEX = re.compile("TIME\s*=\s*%%")


class MysqlDG(object):
    srv = None

    @classmethod
    def get_service(cls, name):
        if cls.srv is not None:
            return cls.srv
        else:
            cls.srv = Service(name)
            return cls.srv


def quantile_packer(iterator):
    qpack = collections.defaultdict(int)
    count = 0
    for item in iterator:
        qpack[int(item)] += 1
        count += 1
    if len(qpack.keys()) == 0:
        qpack[0] = 0  # Special for vyacheslav
    return {"data": sorted(qpack.iteritems()), "count": count}


def merge(iterator):
    count = 0
    res = collections.defaultdict(int)
    for item in iterator:
        count += item.get("count", 0)
        for quantvalue, quantcount in item.get("data", []):
            res[quantvalue] += quantcount
    return {"count": count, "data": res}


def quants(qts, it):
    qts = list(qts)
    if len(qts) == 0:
        return None
    qts.sort(reverse=True)
    res = []
    size = len(qts)
    # first step initialization
    lim = qts.pop()
    summ = 0
    for i in sorted(it.iteritems()):
        if summ >= lim:
            res.append(i[0])
            while qts:
                lim = qts.pop()
                if summ >= lim:
                    res.append(i[0])
                else:
                    break
            if len(res) == size:
                return res
        summ += i[1]
    return res


def aggregate_host(request, response):
    raw = yield request.read()
    #cfg, dgcfg, token, prtime, currtime = msgpack.unpackb(raw)
    TASK = msgpack.unpackb(raw)
    Log.info("%s Handle task" % TASK['id'])
    cfg = TASK['config']  # config of aggregator
    dgcfg = TASK['dgconfig']
    token = TASK['token']
    #prtime = TASK['prevtime']
    #currtime = TASK['currtime']
    #taskId = TASK['id']
    Log.debug(str(cfg))
    dg = MysqlDG.get_service(dgcfg['type'])
    q = TABLEREGEX.sub(token, cfg['query'])
    q = TIMEREGEX.sub("1=1", q)
    Log.info("%s QUERY: %s" % (TASK['id'], q))
    res = yield dg.enqueue("query",
                           msgpack.packb((token, q)))
    #Log.info("Data from DG " + str(res))
    ret = quantile_packer(itertools.chain(*res))
    Log.info("%s Return " % TASK['id'] + str(ret))
    response.write(msgpack.packb(ret))
    response.close()


def aggregate_group(request, response):
    raw = yield request.read()
    inc = msgpack.unpackb(raw)
    cfg, data = inc
    Log.debug("Unpack raw data successfully")
    raw_data = map(msgpack.unpackb, data)
    ret = merge(raw_data)
    Log.debug("Data has been merged %s" % ret)
    qts = map(int,
              map(lambda x: float(ret["count"]) * x / 100,
                  cfg.get("values", [75, 90, 93, 94, 95, 96, 97, 98, 99])))
    try:
        ret = quants(qts,
                     ret['data'])
    except Exception as err:
        Log.error(str(err))
        response.error(100, repr(err))
    else:
        Log.info("Result of group aggreagtion " + str(ret))
        response.write(ret)
        response.close()


if __name__ == '__main__':
    W = Worker()
    W.run({"aggregate_host": aggregate_host,
           "aggregate_group": aggregate_group})
