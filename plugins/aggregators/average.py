#!/usr/bin/env python
import cPickle
import re

import msgpack

from cocaine.worker import Worker
from cocaine.services import Service

from combaine.common.logger import get_logger_adapter

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
    taskId = TASK['id']
    logger = get_logger_adapter(taskId)
    logger.info("Handle task")
    cfg = TASK['config']  # config of aggregator
    token = TASK['token']
    prtime = TASK['prevtime']
    currtime = TASK['currtime']
    dg = MysqlDG.get_service(DATABASEAPP)
    q = TABLEREGEX.sub(token, cfg['query'])
    q = TIMEREGEX.sub("1=1", q)
    logger.debug("QUERY: %s", q)
    pickled_res = yield dg.enqueue("query",
                                   msgpack.packb((token, q)))
    res = cPickle.loads(pickled_res)
    logger.debug(str(res))
    try:
        ret = float(res[0][0])   # SELECT COUNT(*)
        logger.info("Result from DG %s", ret)
        if cfg.get('rps'):
            ret = ret / (currtime - prtime)
    except Exception:
        ret = 0
    logger.info("%s", ret)
    response.write(msgpack.packb(ret))
    response.close()


def aggregate_group(request, response):
    raw = yield request.read()
    tid, cfg, data = msgpack.unpackb(raw)
    logger = get_logger_adapter(tid)
    logger.info("Raw data is received %s", str(data))
    res = sum(map(msgpack.unpackb, data))
    logger.info("Solved %s", str(res))
    response.write(res)
    response.close()


if __name__ == '__main__':
    W = Worker()
    W.run({"aggregate_host": aggregate_host,
           "aggregate_group": aggregate_group})
