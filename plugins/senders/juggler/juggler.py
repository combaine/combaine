#!/usr/bin/env python
import re
import yaml
import collections
import urllib

import msgpack

from tornado.httpclient import AsyncHTTPClient
from tornado.httpclient import HTTPError
from tornado import template
from tornado.ioloop import IOLoop

from cocaine.futures import chain
from cocaine.worker import Worker
from cocaine.logging import Logger
from cocaine.services import Service

LEVELS = ("INFO", "WARN", "CRIT", "OK")

STATUSES = {0: "OK",
            3: "INFO",
            1: "WARN",
            2: "CRIT"}

REVERSE_STATUSES = dict((v, k) for k, v in STATUSES.iteritems())

HTTP_CLIENT = AsyncHTTPClient()

ADD_CHECK = "http://{juggler}/api/checks/set_check?host_name={host}&\
service_name={service}&description={description}&aggregator={aggregator}&do=1"

ADD_CHILD = "http://{juggler}/api/checks/add_child?host_name={host}&\
service_name={service}&child={child}:{service}&do=1"

ADD_METHOD = "http://{juggler}/api/checks/add_methods?host_name={host}&\
service_name={service}&methods_list={methods}&do=1"

EMIT_EVENT = "http://{juggler}/api/events/add_event_proxy?host_name={host}&\
service_name={service}&description={description}&\
instance_name&status={level}&do=1"

log = Logger()


class Juggler(object):

    pattern = re.compile(r"\${\s*([^}\s]*)\s*}")

    def __init__(self, **cfg):
        self.log = log
        for level in LEVELS:
            setattr(self, level, self._convert_templates(cfg.get(level, [])))
        self.checkname = cfg["checkname"]
        self.Aggregator = cfg['Aggregator']
        self.Host = cfg['Host']
        self.Method = cfg['Method']
        self.description = cfg.get("description", "no description")
        self.juggler_hosts = cfg['Juggler_hosts']

    def _convert_templates(self, jtemplates):
        """ Convert my own templates to Tornado templates.
            Add underline prefix to allow using of
            variables starts with digit (50x)
        """
        return map(template.Template, ["{{ %s }}" % re.sub(self.pattern,
                                                           "_\g<1>",
                                                           templ)
                                       for templ in jtemplates])

    @chain.source
    def Do(self, data):
        packed = collections.defaultdict(dict)
        for aggname, subgroups in data.iteritems():
            for subgroup, value in subgroups.iteritems():
                packed[subgroup]["_" + aggname] = value

        for subgroup, value in packed.iteritems():
            log.info("%s" % subgroup)
            if self.check(value, subgroup, "CRIT"):
                log.info("CRIT")
            elif self.check(value, subgroup, "WARN"):
                log.info("WARn")
            elif self.check(value, subgroup, "INFO"):
                log.info("INFO")
            elif self.check(value, subgroup, "OK"):
                log.info("OK")
            else:
                log.info("Send ok manually")
                yield self.send_point("%s-%s" % (self.Host, subgroup),
                                      REVERSE_STATUSES["OK"])
                #HTTP_CLIENT.fetch("http://www.google.com/", self.on_resp)

    def on_resp(self, resp):
        log.info("RESP %s" % resp.code)

    def check(self, value, subgroup, level):
        checks = getattr(self, level, [])
        if len(checks) == 0:
            return False
        for check in checks:
            try:
                print check, value
                if (check.generate(**value) == "False"):
                    return False
            except Exception as err:
                self.log.error(repr(err))
                return False
        IOLoop.current().add_callback(self.send_point,
                                      "%s-%s" % (self.Host, subgroup),
                                      REVERSE_STATUSES[level])
        return True

    #self, level, data, name, status
    @chain.source
    def send_point(self, name, status):
        params = {"host": name,
                  "service": urllib.quote(self.checkname),
                  "description": urllib.quote(self.description),
                  "level": STATUSES[status]}
        yield self.add_check_if_need("CHILD")
        # Emit event
        params["level"] = status
        try:
            futures = list()
            for jhost in self.juggler_hosts:
                params["juggler"] = jhost
                url = EMIT_EVENT.format(**params)
                self.log.info("Send event %s" % url)
                futures.append(HTTP_CLIENT.fetch(url))

            for future in futures:
                try:
                    res = yield future
                    self.log.info(str(res))
                except HTTPError as err:
                    self.log.error(repr(err))
        except Exception as err:
            print err

    @chain.source
    def add_check_if_need(self, host):
        params = {"host": self.Host,
                  "service": urllib.quote(self.checkname),
                  "description": urllib.quote(self.description),
                  "methods": self.Method,
                  "child": host,
                  "aggregator": self.Aggregator}

        # Add checks
        futures = list()
        for jhost in self.juggler_hosts:
            params["juggler"] = jhost
            url = ADD_CHECK.format(**params)
            log.info("Add check %s" % url)
            futures.append(HTTP_CLIENT.fetch(url))

        for future in futures:
            try:
                res = yield future
                print res
            except HTTPError as err:
                print err

        # Add childs
        futures = list()
        for jhost in self.juggler_hosts:
            params["juggler"] = jhost
            url = ADD_CHILD.format(**params)
            self.log.info("Add child %s" % url)
            futures.append(HTTP_CLIENT.fetch(url))

        for future in futures:
            try:
                res = yield future
            except HTTPError as err:
                log.error(repr(err))

        # Add methods
        futures = list()
        for jhost in self.juggler_hosts:
            params["juggler"] = jhost
            url = ADD_METHOD.format(**params)
            self.log.info("add method %s" % url)
            futures.append(HTTP_CLIENT.fetch(url))

        for future in futures:
            try:
                res = yield future
            except HTTPError as err:
                log.error(repr(err))

        yield True


def send(request, response):
    raw = yield request.read()
    task = msgpack.unpackb(raw)
    log.info("%s" % str(task))
    raw_cfg = yield Service("cfgmanager").enqueue("common", "")
    cfg = yaml.load(raw_cfg)
    juggler_hosts = cfg['cloud_config']['juggler_hosts']
    task['Config']['Juggler_hosts'] = juggler_hosts
    jc = Juggler(**task["Config"])
    try:
        yield jc.Do(task["Data"])
    except Exception as err:
        log.error(err)
    response.write("ok")
    response.close()

if __name__ == "__main__":
    W = Worker()
    W.run({"send": send})
