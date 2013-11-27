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
        except Exception as err:
            return None
        else:
            self._cache[name] = s
            return s

cache = Cache()

def split_hosts_by_dc(http_hand_url, groupname):
    hosts = urllib.urlopen("%s%s?fields=root_datacenter_name,fqdn" % (http_hand_url, groupname)).read()
    if hosts == 'No groups found':
        return {}
    host_dict = collections.defaultdict(list)
    for item in hosts.splitlines():
        dc, host = item.split('\t')
        host_dict["%s-%s" % (groupname, dc)].append(host)
    return host_dict

#{'Group': 'photo-proxy', 'CurrTime': -1, 'Config': 'http_ok_timings', 'Id': '', 'PrevTime': -1}

# Get hosts by group
# Gen keys
# Read keys
# 
def aggreagate(request, response):
    raw = yield request.read()
    task = msgpack.unpackb(raw)
    log.info("Task %s" % task)
    raw = yield cfgmanager.enqueue("aggregate", task['Config'])
    aggcfg = yaml.load(raw)
    log.info("Config %s" % aggcfg)
    commoncfg = yield cfgmanager.enqueue("common", "")
    httphand = yaml.load(commoncfg)['Combainer']['Main']['HTTP_HAND']
    hosts = split_hosts_by_dc(httphand, task['Group'])
    log.info(str(hosts))

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
                key = "%s;%s;%s;%s;%s" % (host, task['PConfig'], task['Config'], name, task['CurrTime'])
                log.error(key)
                try:
                    data = yield storage.read("combaine", key)
                    log.error("Read key %s" % data)
                    subgroup_data.append(data)
                except Exception:
                    log.error("Unable to read %s" % key)
            mapping[subgroup] = subgroup_data
            res = yield app.enqueue("aggregate_group", msgpack.packb((cfg, subgroup_data)))
            log.error("name %s subgroup %s result %s, %d" % (name, subgroup, res, len(subgroup_data)))
            result[name][subgroup]=res

        all_data = []
        for v in mapping.itervalues():
            all_data.extend(v)
        res = yield app.enqueue("aggregate_group", msgpack.packb((cfg, all_data)))
        log.error("name %s ALL %s %d" % (name, res, len(all_data)))
        result[name][task['Group']] = res

    futures = []
    for name, item in aggcfg.get('senders', {}).iteritems():
        try:
            s = Service(item.get("type", "MISSING"))
        except Exception as err:
            log.error(str(err))
        else:
            futures.append(s.enqueue("send", msgpack.packb({"Config": item, "Data": result})))

    for fut in futures:
        try:
            res = yield fut
            log.info("res %s" % res)
        except Exception as err:
            log.error(str(err))

    log.error("DONE")
    log.info("Result %s" % str(result))
    response.write("ok")        
    response.close()


if __name__ == "__main__":
    W = Worker()
    W.run({"handleTask": aggreagate})
    

