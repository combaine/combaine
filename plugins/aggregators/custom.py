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

LOG = Logger()
LOG.error("INITIALIZE")

LOG = logging.getLogger("combaine")
LOG.setLevel(logging.DEBUG)
CLH = CocaineHandler()
CLH.setFormatter(logging.Formatter("%(tid)s %(message)s"))
CLH.setLevel(logging.DEBUG)
LOG.addHandler(CLH)

PATH = os.environ.get('PLUGINS_PATH', '/usr/lib/yandex/combaine/custom')
sys.path.insert(0, PATH)

EXTS = [ext for (ext, _, _) in imp.get_suffixes()]

RELOAD_PLUGIN_INTERVAL = 180  # seconds
LAST_PLUGIN_LOAD_TIME = time() - (RELOAD_PLUGIN_INTERVAL + 1) # trigger update


def _is_plugin(candidate):
    name, extension = os.path.splitext(candidate)
    return name != "__init__" and extension in EXTS


def _is_candidate(name):
    return not name.startswith("_") and name[0].isupper()


def plugin_import():
    """
    It tries to import the extensions of a custom aggregator
    in the private namespace and select all the names starting
    with an uppercase letter
    """

    if LAST_PLUGIN_LOAD_TIME + RELOAD_PLUGIN_INTERVAL > time():
        return plugin_import.all_custom_parsers

    parsers = {}
    logger = logging.LoggerAdapter(LOG, {"tid": "plugin_import"})
    modules = set(os.path.splitext(c)[0] for c in os.listdir(PATH) if _is_plugin(c))
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
    plugin_import.all_custom_parsers = parsers
    return parsers


def aggregate_host(request, response):
    """
    Gets the result of a single host,
    performs parsing and their aggregation
    """
    try:
        raw = yield request.read()
        task = msgpack.unpackb(raw)
        logger = logging.LoggerAdapter(LOG, {"tid": task['Id']})

        payload = task['Data']
        cfg = task['Config']
        klass_name = cfg['class']
        cfg['logger'] = logger

        result = _aggregate_host(klass_name, payload, cfg, task)
        response.write(msgpack.packb(result))
    except KeyError:
        response.error(-100, "There's no class named %s" % klass_name)
        logger.error("class %s is absent", klass_name)
    except Exception as err:  # pylint: disable=broad-except
        response.error(-3, "Exception during handling %s" % repr(err))
        logger.error("Error %s", err)
    finally:
        response.close()


def aggregate_group(request, response):
    """
    Receives a list of results from the aggregate_host,
    and performs aggregation by group
    """
    logger = LOG
    try:
        raw = yield request.read()
        task = msgpack.unpackb(raw)
        logger = logging.LoggerAdapter(LOG, {'tid': task['Id']})
        payload = map(msgpack.unpackb, task['Data'])
        cfg = task['Config']
        klass_name = cfg['class']
        cfg['logger'] = logger
        result = _aggregate_group(klass_name, payload, cfg)
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


def _aggregate_host(klass_name, payload, config, task):
    available = plugin_import()
    klass = available[klass_name]
    handler = klass(config)
    prevtime, currtime = task["PrevTime"], task["CurrTime"]
    return handler.aggregate_host(payload, prevtime, currtime)


def _aggregate_group(klass_name, payload, config):
    available = plugin_import()
    klass = available[klass_name]
    handler = klass(config)
    return handler.aggregate_group(payload)


if __name__ == '__main__':
    W = Worker()
    W.run({"aggregate_host": aggregate_host,
           "aggregate_group": aggregate_group})
