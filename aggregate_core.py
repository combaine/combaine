#!/usr/bin/env python
import collections

import msgpack
import socket
import yaml
from tornado.httpclient import AsyncHTTPClient
from tornado.httpclient import HTTPError

from cocaine.worker import Worker
from cocaine.services import Service
from cocaine.asio.engine import asynchronous

from combaine.common.logger import get_logger_adapter
from combaine.common import AggregationTask

from cocaine.logging import Logger

log = Logger()


cfgmanager = Service("cfgmanager")
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

storage_cache = Service("storage")
ST_CACHE_NAMESPACE = "aggregate_st_cache_namespace"


HTTP_CLIENT = AsyncHTTPClient()


@asynchronous
def split_hosts_by_dc(http_hand_url, groupname):
    url = http_hand_url % groupname
    log.info(url)
    try:
        response = yield HTTP_CLIENT.fetch(url,
                                           connect_timeout=1.0,
                                           request_timeout=1.0)
        hosts = response.body
        if hosts == 'No groups found':
            raise Exception(hosts)
    except (HTTPError, socket.error) as err:
        log.info("Unable to fetch groups %s. Cache is used" % str(err))
        print "Cache is used"
        hosts = yield storage_cache.read(ST_CACHE_NAMESPACE, groupname)
    else:
        yield storage_cache.write(ST_CACHE_NAMESPACE, groupname, hosts)

    host_dict = collections.defaultdict(list)
    for item in hosts.splitlines():
        dc, host = item.split('\t')
        host_dict[dc].append(host)
    yield host_dict


# type AggregationTask struct {
#     CommonTask
#     // Name of the current aggregation config
#     Config string
#     // Name of handled parsing config
#     ParsingConfigName string
#     // Content of the current parsing config
#     ParsingConfig configs.ParsingConfig
#     // Current aggregation config
#     AggregationConfig configs.AggregationConfig
# }
def aggreagate(request, response):
    raw = yield request.read()
    task = AggregationTask(raw)
    logger = get_logger_adapter(task.Id)
    logger.info("Task has started")
    METAHOST = task.parsing_config.metahost
    GROUP = task.parsing_config.cfg['Groups'][0]

    # read aggregation config passed to us
    aggcfg = task.aggregation_config
    logger.debug("aggregation config %s", aggcfg)

    # read combaine.yaml to get and decode it to get HTTP_HAND
    # to get hosts for given group. Seems it's better to pass them
    # with the task
    commoncfg = yield cfgmanager.enqueue("common", "")
    httphand = yaml.load(commoncfg)['Combainer']['Main']['HTTP_HAND']
    hosts = yield split_hosts_by_dc(httphand, GROUP)
    logger.info("%s", hosts)
    # repack hosts by subgroups by dc
    # For example:
    # {"GROUP-DC": "hostname"} from {"DC": "hostname"}
    hosts = dict(("%s-%s" % (METAHOST, subgroup), v)
                 for subgroup, v in hosts.iteritems())

    result = {}

    for name, cfg in aggcfg.data.iteritems():
        mapping = {}

        logger.info("Send to %s %s" % (name, cfg['type']))
        app = cache.get(cfg['type'])
        if app is None:
            log.info("Skip %s" % cfg['type'])
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
            res = yield app.enqueue("aggregate_group",
                                    msgpack.packb((cfg, subgroup_data)))
            logger.info("name %s subgroup %s result %s", name, subgroup, res)
            result[name][subgroup] = res

        all_data = []
        for v in mapping.itervalues():
            all_data.extend(v)
        res = yield app.enqueue("aggregate_group",
                                msgpack.packb((cfg, all_data)))
        logger.info("name %s ALL %s %d" % (name, res, len(all_data)))
        result[name][METAHOST] = res

    # Send data to various senders
    try:
        for name, item in aggcfg.senders.iteritems():
            try:
                sender_type = item.get("type")
                if sender_type is None:
                    logger.error("unable to detect sender type: %s", name)
                    continue

                logger.info("Send to %s", sender_type)
                s = Service(sender_type)
            except Exception as err:
                logger.error(err)
            else:
                res = yield s.enqueue("send", msgpack.packb({"Config": item,
                                                             "Data": result,
                                                             "Id": task.Id}))
                logger.info("res for %s is %s", sender_type, res)
    except Exception as err:
        logger.exception("%s %s %s", err, aggcfg, aggcfg.senders)

    logger.info("Result %s", result)
    response.write("Done %s" % task.Id)
    response.close()


if __name__ == "__main__":
    W = Worker()
    W.run({"handleTask": aggreagate})
