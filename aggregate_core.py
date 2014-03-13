#!/usr/bin/env python
import urllib
import collections

import msgpack
import yaml

from cocaine.worker import Worker
from cocaine.services import Service

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


def split_hosts_by_dc(http_hand_url, groupname):
    url = "%s%s" % (http_hand_url, groupname)
    log.info(url)
    hosts = urllib.urlopen(url).read()
    if hosts == 'No groups found':
        return {}
    host_dict = collections.defaultdict(list)
    for item in hosts.splitlines():
        dc, host = item.split(' ')
        host_dict[dc].append(host)
    return host_dict


def aggreagate(request, response):
    raw = yield request.read()
    task = msgpack.unpackb(raw)
    log.info("Start task %s" % task["Id"])
    ID = task["Id"]
    METAHOST = task["Metahost"]
    raw = yield cfgmanager.enqueue("aggregate", task['Config'])
    aggcfg = yaml.load(raw)
    log.debug("%s Config %s" % (ID, aggcfg))
    commoncfg = yield cfgmanager.enqueue("common", "")
    httphand = yaml.load(commoncfg)['Combainer']['Main']['HTTP_HAND']
    hosts = split_hosts_by_dc(httphand, task['Group'])
    log.info("%s %s" % (ID, hosts))
    hosts = dict(("%s-%s" % (METAHOST, subgroup), v) for subgroup, v in hosts.iteritems())

    result = {}

    for name, cfg in aggcfg.get('data', {}).iteritems():
        mapping = {}

        log.info("Send to %s %s" % (name, cfg['type']))
        app = cache.get(cfg['type'])
        if app is None:
            log.info("Skip %s" % cfg['type'])
            continue

        result[name] = {}

        for subgroup, value in hosts.iteritems():
            subgroup_data = list()
            for host in value:
                key = "%s;%s;%s;%s;%s" % (host, task['PConfig'],
                                          task['Config'],
                                          name,
                                          task['CurrTime'])
                try:
                    data = yield storage.read("combaine", key)
                    subgroup_data.append(data)
                    if cfg.get("perHost"):
                        res = yield app.enqueue("aggregate_group",
                                                msgpack.packb((cfg, [data])))
                        result[name][host] = res
                except Exception as err:
                    if err.code != 2:
                        log.error("%s Unable to read from elliptics cache %s %s" %
                                  (ID, key, repr(err)))

            mapping[subgroup] = subgroup_data
            res = yield app.enqueue("aggregate_group",
                                    msgpack.packb((cfg, subgroup_data)))
            log.info("%s name %s subgroup %s result %s" % (ID,
                                                           name,
                                                           subgroup,
                                                           res))
            result[name][subgroup] = res

        all_data = []
        for v in mapping.itervalues():
            all_data.extend(v)
        res = yield app.enqueue("aggregate_group",
                                msgpack.packb((cfg, all_data)))
        log.info("name %s ALL %s %d" % (name, res, len(all_data)))
        result[name][METAHOST] = res

    for name, item in aggcfg.get('senders', {}).iteritems():
        try:
            sender_type = item.get("type", "MISSING")
            log.info("Send to %s" % sender_type)
            s = Service(sender_type)
        except Exception as err:
            log.error(str(err))
        else:
            res = yield s.enqueue("send", msgpack.packb({"Config": item,
                                                         "Data": result}))
            log.info("res for %s is %s" % (sender_type, res))

    log.info("%s Result %s" % (ID, result))
    response.write("Done %s" % task["Id"])
    response.close()


if __name__ == "__main__":
    W = Worker()
    W.run({"handleTask": aggreagate})
