#!/usr/bin/env python

import msgpack

from cocaine.worker import Worker
from cocaine.services import Service

from combaine.common.logger import get_logger_adapter
from combaine.common import AggregationTask


storage = Service("elliptics")


class Cache(object):

    def __init__(self):
        self._cache = {}

    def get(self, name):
        return self._cache.get(name) or self.put(name)

    def put(self, name):
        try:
            s = Service(name)
        except Exception:
            return None
        else:
            self._cache[name] = s
            return s

cache = Cache()


def aggreagate(request, response):
    raw = yield request.read()
    task = AggregationTask(raw)
    logger = get_logger_adapter(task.Id)
    logger.info("task started")
    metahost = task.parsing_config.metahost
    hosts = task.hosts

    # read aggregation config passed to us
    aggcfg = task.aggregation_config
    logger.debug("aggregation config %s", aggcfg)

    logger.info("%s", hosts)
    # repack hosts by subgroups by dc
    # For example:
    # {"GROUP-DC": "hostname"} from {"DC": "hostname"}
    hosts = dict(("%s-%s" % (metahost, subgroup), v)
                 for subgroup, v in hosts.iteritems())

    result = {}

    for name, cfg in aggcfg.data.iteritems():
        mapping = {}

        logger.info("Send to %s %s" % (name, cfg['type']))
        app = cache.get(cfg['type'])
        if app is None:
            logger.info("Skip %s" % cfg['type'])
            continue

        result[name] = {}

        for subgroup, value in hosts.iteritems():
            subgroup_data = list()
            for host in value:
                # Key specification
                key = "%s;%s;%s;%s;%s" % (host, task.parsing_config_name,
                                          task.aggregation_config_name,
                                          name,
                                          task.CurrTime)
                try:
                    data = yield storage.read("combaine", key)
                    subgroup_data.append(data)
                    if cfg.get("perHost"):
                        res = yield app.enqueue("aggregate_group",
                                                msgpack.packb((cfg, [data])))
                        result[name][host] = res
                except Exception as err:
                    if err.code != 2:
                        logger.error("unable to read from cache %s %s",
                                     key, err)

            mapping[subgroup] = subgroup_data
            try:
                res = yield app.enqueue("aggregate_group",
                                        msgpack.packb((cfg, subgroup_data)))
                logger.info("name %s subgroup %s result %s",
                            name, subgroup, res)
                result[name][subgroup] = res
            except Exception as err:
                logger.error("unable to aggregte %s %s %s",
                             name, subgroup, err)

        all_data = []
        for v in mapping.itervalues():
            all_data.extend(v)
        try:
            res = yield app.enqueue("aggregate_group",
                                    msgpack.packb((cfg, all_data)))
        except Exception as err:
            logger.error("unable to aggreagate all: %s %s", name, err)
        logger.info("name %s ALL %s %d" % (name, res, len(all_data)))
        result[name][metahost] = res

    # Send data to various senders
    for name, item in aggcfg.senders.iteritems():
        try:
            sender_type = item.get("type")
            if sender_type is None:
                logger.error("unable to detect sender type: %s", name)
                continue

            logger.info("Send to %s", sender_type)
            s = Service(sender_type)
            res = yield s.enqueue("send", msgpack.packb({"Config": item,
                                                         "Data": result,
                                                         "Id": task.Id}))
            logger.info("res for %s is %s", sender_type, res)
        except Exception as err:
            logger.error("unable to send to %s %s", name, err)

    logger.info("Result %s", result)
    response.write("Done %s" % task.Id)
    response.close()


if __name__ == "__main__":
    W = Worker()
    W.run({"handleTask": aggreagate})
