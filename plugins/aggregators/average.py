#!/usr/bin/env python
import cPickle
import re

import msgpack

from cocaine.worker import Worker
from cocaine.logging import Logger
from cocaine.services import Service

Log = Logger()

TABLEREGEX = re.compile("%TABLENAME%")
TIMEREGEX = re.compile("TIME\s*=\s*%%")
DATABASEAPP = "mysqldg"


class MysqlDG(object):
    srv = None

    @classmethod
    def get_service(cls, name):
        if cls.srv is not None:
            return cls.srv
        else:
            cls.srv = Service(name)
            return cls.srv


def aggregate_host(request, response):
    raw = yield request.read()
    TASK = msgpack.unpackb(raw)
    Log.info("%s Handle task" % TASK['id'])
    cfg = TASK['config']  # config of aggregator
    token = TASK['token']
    prtime = TASK['prevtime']
    currtime = TASK['currtime']
    taskId = TASK['id']
    dg = MysqlDG.get_service(DATABASEAPP)
    q = TABLEREGEX.sub(token, cfg['query'])
    q = TIMEREGEX.sub("1=1", q)
    Log.debug("%s QUERY: %s" % (taskId, q))
    pickled_res = yield dg.enqueue("query",
                                   msgpack.packb((token, q)))
    res = cPickle.loads(pickled_res)
    Log.debug(str(res))
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
    Log.info("Raw data is received %s" % str(inc))
    res = sum(map(msgpack.unpackb, data))
    Log.info("Solved %s" % res)
    response.write(res)
    response.close()


if __name__ == '__main__':
    W = Worker()
    W.run({"aggregate_host": aggregate_host,
           "aggregate_group": aggregate_group})
