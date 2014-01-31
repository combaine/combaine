#!/usr/bin/env python
import re

import msgpack

from cocaine.worker import Worker
from cocaine.logging import Logger
from cocaine.services import Service

Log = Logger()

TABLEREGEX = re.compile("%TABLENAME%")
TIMEREGEX = re.compile("TIME\s*=\s*%%")


def aggregate_host(request, response):
    raw = yield request.read()
    #cfg, dgcfg, token, prtime, currtime = msgpack.unpackb(raw)
    TASK = msgpack.unpackb(raw)
    Log.info("Handle task %s" % TASK)
    cfg = TASK['config']  # config of aggregator
    dgcfg = TASK['dgconfig']
    token = TASK['token']
    prtime = TASK['prevtime']
    currtime = TASK['currtime']
    taskId = TASK['id']
    dg = Service(dgcfg['type'])
    q = TABLEREGEX.sub(token, cfg['query'])
    q = TIMEREGEX.sub("1=1", q)
    Log.info("%s QUERY: %s" % (taskId, q))
    res = yield dg.enqueue("query",
                           msgpack.packb((dgcfg,
                                          token,
                                          q)))

    try:
        ret = float(res[0][0])   # SELECT COUNT(*)
        Log.info("%s Result from DG %s" % (taskId, ret))
        if cfg.get('rps'):
            ret = ret / (currtime - prtime)
    except Exception:
        ret = 0
    Log.info("%s %s" % (taskId, ret))
    response.write(msgpack.packb(ret))
    response.close()


def aggregate_group(request, response):
    raw = yield request.read()
    inc = msgpack.unpackb(raw)
    cfg, data = inc
    Log.info("Receive raw result %s" % str(inc))
    res = sum(map(msgpack.unpackb, data))
    Log.info("Receive result %s" % res)
    response.write(res)
    response.close()


if __name__ == '__main__':
    W = Worker()
    W.run({"aggregate_host": aggregate_host,
           "aggregate_group": aggregate_group})
