#!/usr/bin/env python
import json
import re
import yaml
import collections
import urllib

import msgpack

from tornado.httpclient import AsyncHTTPClient
from tornado.httpclient import HTTPError
from tornado.httputil import HTTPHeaders
from tornado.ioloop import IOLoop

from cocaine.futures import chain
from cocaine.worker import Worker
from cocaine.logging import Logger

LEVELS = ("INFO", "WARN", "CRIT", "OK")

STATUSES = {0: "OK",
            3: "INFO",
            1: "WARN",
            2: "CRIT"}

DEFAULT_HEADERS = HTTPHeaders({"User-Agent": "Yandex/CombaineClient"})
DEFAULT_AGGREGATOR_KWARGS = {"ignore_nodata": 1}

REVERSE_STATUSES = dict((v, k) for k, v in STATUSES.iteritems())

HTTP_CLIENT = AsyncHTTPClient()

CHECK_CHECK = "http://{juggler}/api/checks/has_check?host_name={host}&\
service_name={service}&do=1"

LIST_CHILD = "http://{juggler}/api/checks/children_tree?host_name={host}&\
service_name={service}&do=1"

ADD_CHECK = "http://{juggler}/api/checks/set_check?host_name={host}&\
service_name={service}&description={description}&\
aggregator_kwargs={aggregator_kwargs}&\
aggregator={aggregator}&do=1"

ADD_CHILD = "http://{juggler}/api/checks/add_child?host_name={host}&\
service_name={service}&child={child}:{service}&do=1"

ADD_METHOD = "http://{juggler}/api/checks/add_methods?host_name={host}&\
service_name={service}&methods_list={methods}&do=1"

EMIT_EVENT = "http://{juggler_frontend}/juggler-fcgi.py?status={level}&\
description={description}&service={service}&instance=&host={host}"

log = Logger()


def upper_dict(dict_object):
    return dict((k.upper(), v) for (k, v) in dict_object.iteritems())


class WrappedLogger(object):
    def __init__(self, extra, logger):
        self.extra = extra
        self.log = logger

    def debug(self, data):
        self.log.debug("%s %s" % (self.extra, data))

    def info(self, data):
        self.log.info("%s %s" % (self.extra, data))

    def warn(self, data):
        self.log.warn("%s %s" % (self.extra, data))

    def error(self, data):
        self.log.error("%s %s" % (self.extra, data))


class Juggler(object):

    pattern = re.compile(r"\${\s*([^}\s]*)\s*}")

    def __init__(self, **cfg):
        # uppercase all keys
        cfg = upper_dict(cfg)
        ID = cfg.get("ID", "dummyID")
        self.log = WrappedLogger(ID, log)
        for level in LEVELS:
            setattr(self, level, cfg.get(level, []))

        self.checkname = cfg['CHECKNAME']
        self.Aggregator = cfg['AGGREGATOR']
        self.Host = cfg['HOST']
        # ugly hack for changes juggler API:
        # &methods_list=GOLEM,SMS to &methods_list=GOLEM&methods_list=SMS
        placeholder = "&methods_list="
        self.Method = placeholder.join(cfg['METHOD'].split(","))
        self.description = cfg.get('DESCRIPTION', "no description")
        self.juggler_hosts = cfg['JUGGLER_HOSTS']
        self.juggler_frontend = cfg['JUGGLER_FRONTEND']
        self.aggregator_kwargs = json.dumps(cfg.get('AGGREGATOR_KWARGS',
                                                    DEFAULT_AGGREGATOR_KWARGS))

    def Do(self, data):
        packed = collections.defaultdict(dict)
        for aggname, subgroups in data.iteritems():
            for subgroup, value in subgroups.iteritems():
                packed[subgroup][aggname] = value

        for subgroup, value in packed.iteritems():
            self.log.debug("Habdling subgroup %s" % subgroup)
            if self.check(value, subgroup, "CRIT"):
                self.log.debug("CRIT")
            elif self.check(value, subgroup, "WARN"):
                self.log.debug("WARN")
            elif self.check(value, subgroup, "INFO"):
                self.log.debug("INFO")
            elif self.check(value, subgroup, "OK"):
                self.log.debug("OK")
            else:
                self.log.debug("Send ok manually")
                IOLoop.current().add_callback(self.send_point,
                                              "%s-%s" % (self.Host, subgroup),
                                              REVERSE_STATUSES["OK"])
        return True

    def on_resp(self, resp):
        self.log.info("RESP %s" % resp.code)

    def check(self, data, subgroup, level):
        checks = getattr(self, level, [])
        if len(checks) == 0:
            return False

        # Checks are coupled with OR logic.
        # Point will be sent
        # if even one of expressions is evaluated as True
        for check in checks:
            try:
                # prepare evaluation string
                # move to a separate function
                code = check
                for key, value in data.iteritems():
                    code, _ = re.subn(r"\${%s}" % key, str(value), code)

                self.log.debug("After substitution in %s %s" % (check,
                                                                code))
                # evaluate code
                # TBD: make it safer!!!
                res = eval(code)

                self.log.debug("Evaluated result: %s %s" % (check, res))

                # if res looks like True
                # send point and return True
                if res:
                    IOLoop.current().add_callback(self.send_point,
                                                  "%s-%s" % (self.Host,
                                                             subgroup),
                                                  REVERSE_STATUSES[level],
                                                  code)
                    return True
            except SyntaxError as err:
                self.log.error("SyntaxError in expression %s" % code)
            except Exception as err:
                self.log.error(repr(err))
        return False

    #self, level, data, name, status
    @chain.source
    def send_point(self, name, status, trigger_description=None):
        if trigger_description:
            description = "%s trigger: %s" % (self.description,
                                              trigger_description)
        else:
            description = self.description

        params = {"host": name,
                  "service": urllib.quote(self.checkname),
                  "description": urllib.quote(description),
                  "level": STATUSES[status]}

        child = name
        yield self.add_check_if_need(child)

        success = 0
        for jhost in self.juggler_frontend:
            try:
                params["juggler_frontend"] = jhost
                url = EMIT_EVENT.format(**params)
                self.log.info("Send event %s" % url)
                yield HTTP_CLIENT.fetch(url, headers=DEFAULT_HEADERS)
            except HTTPError as err:
                self.log.error(str(err))
                continue
            else:
                self.log.info("Event to %s: OK" % jhost)
                success += 1

        self.log.info("Event has been sent to %d/%d"
                      % (success, len(self.juggler_frontend)))
        yield success

    @chain.source
    def add_check_if_need(self, host):
        params = {"host": self.Host,
                  "service": urllib.quote(self.checkname),
                  "description": urllib.quote(self.description),
                  "methods": self.Method,
                  "child": host,
                  "aggregator": self.Aggregator,
                  "aggregator_kwargs": self.aggregator_kwargs}

        # Add checks
        for jhost in self.juggler_hosts:
            try:
                self.log.info("Work with %s" % jhost)
                params["juggler"] = jhost
                #Check existnace of service
                url = CHECK_CHECK.format(**params)
                self.log.info("Check %s" % url)
                response = yield HTTP_CLIENT.fetch(url,
                                                   headers=DEFAULT_HEADERS)

                # check doesn't exist
                if response.body == "false":
                    url = ADD_CHECK.format(**params)
                    self.log.info("Add check %s" % url)
                    yield HTTP_CLIENT.fetch(url, headers=DEFAULT_HEADERS)

                    url = ADD_CHILD.format(**params)
                    self.log.info("Add child %s" % url)
                    yield HTTP_CLIENT.fetch(url, headers=DEFAULT_HEADERS)

                    url = ADD_METHOD.format(**params)
                    self.log.info("Add method %s" % url)
                    yield HTTP_CLIENT.fetch(url, headers=DEFAULT_HEADERS)
                elif response.body == "true":
                    # check exists, but existance of child must be checked
                    url = LIST_CHILD.format(**params)
                    try:
                        resp = yield HTTP_CLIENT.fetch(url,
                                                       headers=DEFAULT_HEADERS)
                        childs_status = json.loads(resp.body)
                        key = "{child}:{service}".format(**params)

                        self.log.info("checking existance of %s child" % key)
                        if key not in childs_status:
                            # there's no given child for the current check
                            url = ADD_CHILD.format(**params)
                            self.log.info("Add child"
                                          " %s as it does'n exist" % url)
                            yield HTTP_CLIENT.fetch(url,
                                                    headers=DEFAULT_HEADERS)
                    except HTTPError as err:
                        self.log.error("unable to fetch the information"
                                       " about childs %s" % str(err))
                        continue
                    except ValueError as err:
                        self.log.error("unable to decode the information"
                                       " about childs %s" % str(err))
                        continue
                    except Exception as err:
                        self.log.error("unknown error related with the"
                                       " info about childs %s" % str(err))
                        continue
                else:
                    self.log.error("unexpected reply from `has_check`: %s",
                                   response.body)
            except HTTPError as err:
                self.log.error(str(err))
                continue
            except Exception as err:
                self.log.error(str(err))
            else:
                break

        yield True


class JConfig(object):
    config = None
    CONFIG_PATH = "/etc/combaine/juggler.yaml"

    @classmethod
    def get_config(cls):
        return cls.config or cls.load_cfg()

    @classmethod
    def load_cfg(cls):
        with open(cls.CONFIG_PATH, 'r') as f:
            cls.config = yaml.load(f)
        return cls.config


def send(request, response):
    raw = yield request.read()
    task = msgpack.unpackb(raw)
    log.info("%s" % str(task))
    ID = task.get("Id", "MissingID")
    hosts = JConfig.get_config()
    juggler_config = task['Config']
    juggler_config.update(hosts)
    juggler_config['id'] = ID
    jc = Juggler(**juggler_config)

    try:
        jc.Do(task["Data"])
    except Exception as err:
        log.error("%s %s" % (ID, str(err)))
    finally:
        response.write("ok")
        log.info("%s Done" % ID)
        response.close()

if __name__ == "__main__":
    W = Worker()
    W.run({"send": send})
