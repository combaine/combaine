#!/usr/bin/env python

import collections
import httplib
import types
import itertools

from cocaine.worker import Worker

agave_headers = {
        "User-Agent": "Yandex/Agave",
        "Connection": "TE",
        "TE": "deflate,gzip;q=0.3"
}

try:
    agave_hosts = parse_common_cfg('combaine')["cloud_config"]['agave_hosts']
except Exception as err:
    print err

class Agave(object):
    """
    type: agave
    items: [20x, 30x, 50/20x]
    graph_name: http_ok
    graph_template: http_ok
    """
    def __init__(self, **config):
        self.logger = CommonLogger()
        self.graph_name = config.get("graph_name")
        self.graph_template = config.get("graph_template")
        self.fields = config.get("Fields")
        self.template_dict = {  "template" : self.graph_template,
                                "title"    : self.graph_name,
                                "graphname": self.graph_name
                        }
        self.items = config.get('items', [])
        self.logger.debug(self.template_dict)

    def __makeUrls(self, frmt_dict):
        self.template_dict.update(frmt_dict)
        template = "/api/update/%(group)s/%(graphname)s?values=%(values)s&ts=%(time)i&template=%(template)s&title=%(title)s" % self.template_dict
        self.__send_point(template)

    def __send_point2(self, url):
        for agv_host in agave_hosts:
            conn = httplib.HTTPConnection(agv_host, timeout=1)
            headers = agave_headers
            headers['Host'] = agv_host+':80'
            try:
                conn.request("GET", url, None, headers)
                _r = conn.getresponse()
                self.logger.info("%s %s %s %s %s" % (agv_host, _r.status, _r.reason, _r.read().strip('\r\n'), url))
            except Exception as err:
                self.logger.error("Unable to connect to one agave")
            else:
                _r.close()

    def __send_point(self, url):
        AsCli = AsyncHTTP()
        res = AsCli.fetch(dict((agv_host, "http://%s%s" % (agv_host, url)) for agv_host in agave_hosts),\
                             timeout=1)
        for label, ans in res.iteritems():
            self.logger.info("%s %s %s %s" % (ans.request.url,
                                             ans.code,
                                             ans.reason,
                                             ans.body.rstrip('\r\n')))


    def data_filter(self, data):
        return [res for res in data if res.aggname in self.items]

    def send(self, data):
        data = self.data_filter(data)
        for_send = collections.defaultdict(list)
        for aggres in data:
            for sbg_name, val in aggres.values:
                _sbg = sbg_name if sbg_name == aggres.groupname else "-".join((aggres.groupname, sbg_name))
                if isinstance(val, types.ListType): # Quantile
                    l = itertools.izip(self.fields, val)
                    _value = "+".join(("%s:%s" % x for x in l))
                else: # Simle single value
                    _value = "%s:%s" % (aggres.aggname, val)
                for_send[_sbg].append(_value)
                time = aggres.time

        for name, val in for_send.iteritems():
            frmt_dict = { "group"   : name,
                          "values"  : "+".join(val),
                          "time"    : time
            }
            self.__makeUrls(frmt_dict)

PLUGIN_CLASS = Agave


if __name__ == "__main__":
    W = Worker()
