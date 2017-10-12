#!/usr/bin/env python
"""Aggregator with extensions support"""

import os
import sys
import imp

import logging
from time import time

import msgpack
# pylint: disable=import-error
from cocaine.worker import Worker
from cocaine.logging import Logger
from cocaine.logging.hanlders import CocaineHandler
# pylint: enable=import-error

Logger().info("Initialize custom.py")
PATH = os.environ.get('PLUGINS_PATH', '/usr/lib/yandex/combaine/custom')
sys.path.insert(0, PATH)
EXTS = [ext for (ext, _, _) in imp.get_suffixes()]


def _is_plugin(candidate):
    name, extension = os.path.splitext(candidate)
    return name != "__init__" and extension in EXTS


def _is_candidate(name):
    return not name.startswith("_") and name[0].isupper()


class Custom(object):
    """Combaine custom plugin loader"""

    def __init__(self):
        self.log = logging.getLogger("combaine")
        self.log.setLevel(logging.DEBUG)
        clh = CocaineHandler()
        clh.setFormatter(logging.Formatter("%(tid)s %(message)s"))
        clh.setLevel(logging.DEBUG)
        self.log.addHandler(clh)

        self.reload_interval = 180  # seconds
        self.last_load_time = time()
        self.all_custom_parsers = {}
        self.plugin_import()


    def plugin_import(self):
        """
        It tries to import the extensions of a custom aggregator
        in the private namespace and select all the names starting
        with an uppercase letter
        """

        if self.last_load_time + self.reload_interval > time():
            return
        self.last_load_time = time()

        parsers = {}
        logger = logging.LoggerAdapter(self.log, {"tid": "plugin_import"})
        modules = set(
            os.path.splitext(c)[0] for c in os.listdir(PATH) if _is_plugin(c)
        )
        for module in modules:
            try:
                mfp, path, descr = imp.find_module(module, [PATH])
            except ImportError as err:
                logger.error("ImportError. Module: %s %s" % (module, repr(err)))
                continue

            try:
                _temp = imp.load_module(module, mfp, path, descr)
                for item in (x for x in dir(_temp) if _is_candidate(x)):
                    candidate = getattr(_temp, item)
                    if callable(candidate):
                        parsers[item] = candidate
            except ImportError as err:
                logger.error("ImportError. Module: %s %s" % (module, repr(err)))
            except Exception as err:  # pylint: disable=broad-except
                logger.error("Exception. Module: %s %s" % (module, repr(err)))
            finally:
                if mfp:
                    mfp.close()
        logger.debug("%s are available custom plugin for parsing" % parsers.keys())
        self.all_custom_parsers = parsers


    def aggregate_host(self, request, response):
        """
        Gets the result of a single host,
        performs parsing and their aggregation
        """
        logger = self.log
        try:
            raw = yield request.read()
            task = msgpack.unpackb(raw)
            logger = logging.LoggerAdapter(self.log, {"tid": task['Id']})

            payload = task['Data']
            cfg = task['Config']
            klass_name = cfg['class']
            cfg['logger'] = logger

            self.plugin_import()
            klass = self.all_custom_parsers[klass_name]
            prevtime, currtime = task["PrevTime"], task["CurrTime"]
            result = klass(cfg).aggregate_host(payload, prevtime, currtime)

            response.write(msgpack.packb(result))
        except KeyError:
            response.error(-100, "There's no class named %s" % klass_name)
            logger.error("class %s is absent", klass_name)
        except Exception as err:  # pylint: disable=broad-except
            response.error(-3, "Exception during handling %s" % repr(err))
            logger.error("Error %s", err)
        finally:
            response.close()


    def aggregate_group(self, request, response):
        """
        Receives a list of results from the aggregate_host,
        and performs aggregation by group
        """
        logger = self.log
        try:
            raw = yield request.read()
            task = msgpack.unpackb(raw)
            logger = logging.LoggerAdapter(self.log, {'tid': task['Id']})
            payload = map(msgpack.unpackb, task['Data'])
            cfg = task['Config']
            klass_name = cfg['class']
            cfg['logger'] = logger

            self.plugin_import()
            klass = self.all_custom_parsers[klass_name]
            result = klass(cfg).aggregate_group(payload)

            logger.info("Aggregation result %s: %s", str(task['Meta']), str(result))
            response.write(result)
        except KeyError:
            response.error(-100, "There's no class named %s" % klass_name)
            logger.error("class %s is absent", klass_name)
        except Exception as err:  # pylint: disable=broad-except
            response.error(100, repr(err))
            logger.error("Error %s", err)
        finally:
            response.close()


if __name__ == '__main__':
    C = Custom()
    W = Worker()
    W.run({"aggregate_host": C.aggregate_host,
           "aggregate_group": C.aggregate_group})
