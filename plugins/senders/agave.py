#!/usr/bin/env python

import httplib
import types
import time
import collections

import yaml
import msgpack

from cocaine.worker import Worker
from cocaine.logging import Logger
from cocaine.services import Service

agave_headers = {"User-Agent": "Yandex/Agave",
                 "Connection": "TE",
                 "TE": "deflate,gzip;q=0.3"}

agave_hosts = []


class Agave(object):

    def __init__(self, **config):
        self.logger = Logger()
        self.graph_name = config.get("graph_name")
        self.graph_template = config.get("graph_template")
        self.fields = config.get("Fields")
        self.template_dict = {"template": self.graph_template,
                              "title": self.graph_name,
                              "graphname": self.graph_name}
        self.items = config.get('items', [])
        self.logger.debug(self.template_dict)

    def __makeUrls(self, frmt_dict):
        self.template_dict.update(frmt_dict)
        template = "/api/update/%(group)s/%(graphname)s?values=%(values)s&ts=%(time)i&template=%(template)s&title=%(title)s" % self.template_dict
        self.__send_point(template)

    def __send_point(self, url):
        for agv_host in agave_hosts:
            conn = httplib.HTTPConnection(agv_host, timeout=1)
            headers = agave_headers
            headers['Host'] = agv_host + ':80'
            try:
                conn.request("GET", url, None, headers)
                _r = conn.getresponse()
                self.logger.info("%s %s %s %s %s" % (agv_host, _r.status, _r.reason, _r.read().strip('\r\n'), url))
            except Exception:
                self.logger.error("Unable to connect to %s" % agv_host)
            else:
                _r.close()

    def send(self, data):
        self.logger.info(str(data))
        output = collections.defaultdict(list)
        for aggname in self.items:
            values = data.get(aggname)
            if values is None:
                self.logger.warn("Values for %s are missing" % aggname)
                continue
            for subgroup, result in values.iteritems():
                self.logger.info(str(type(result)))
                if isinstance(result, (types.TupleType, types.ListType)):
                    self.logger.warn("Quantile hasn't supported yet")
                    val = None
                    continue
                elif isinstance(result, (types.FloatType,
                                         types.IntType,
                                         types.LongType)):
                    val = "%s:%s" % (aggname, result)
                    self.logger.info(val)
                output[subgroup].append(val)

        for name, val in output.iteritems():
            frmt_dict = {"group": name,
                         "values": "+".join(val),
                         "time": int(time.time())}
            self.__makeUrls(frmt_dict)


def send(request, response):
    global agave_hosts
    req = yield request.read()
    raw_cfg = yield Service("cfgmanager").enqueue("common", "")
    cfg = yaml.load(raw_cfg)
    agave_hosts = cfg['cloud_config']['agave_hosts']
    inc = msgpack.unpackb(req)
    agave = Agave(**inc['Config'])
    agave.send(inc['Data'])
    response.write("ok")
    response.close()


if __name__ == "__main__":
    W = Worker()
    W.run({"send": send})
