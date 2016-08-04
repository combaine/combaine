#!/usr/bin/env python
"""Aggregator with extensions support"""

import os
import sys
import imp

import msgpack
# pylint: disable=import-error
from cocaine.worker import Worker
from cocaine.logging import Logger

from combaine.common.logger import get_logger_adapter
# pylint: enable=import-error

LOG = Logger()

PATH = os.environ.get('PLUGINS_PATH', '/usr/lib/yandex/combaine/custom')
sys.path.insert(0, PATH)

EXTS = [ext for (ext, _, _) in imp.get_suffixes()]


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

    modules = set(os.path.splitext(c)[0] for c in os.listdir(PATH)
                  if _is_plugin(c))
    all_custom_parsers = {}
    for module in modules:
        try:
            mfp, path, descr = imp.find_module(module, [PATH])
        except ImportError:
            continue

        try:
            _temp = imp.load_module(module, mfp, path, descr)
            for item in (x for x in dir(_temp) if _is_candidate(x)):
                candidate = getattr(_temp, item)
                if callable(candidate):
                    all_custom_parsers[item] = candidate
        except ImportError as err:
            LOG.error("ImportError. Module: %s %s" % (module, repr(err)))
        except Exception as err:  # pylint: disable=broad-except
            LOG.error("Exception. Module: %s %s" % (module, repr(err)))
        finally:
            if mfp:
                mfp.close()
    LOG.debug("%s are available custom plugin for parsing"
              % str(all_custom_parsers.keys()))
    return all_custom_parsers


def aggregate_host(request, response):
    """
    Gets the result of a single host,
    performs parsing and their aggregation
    """
    raw = yield request.read()
    task = msgpack.unpackb(raw)
    tid = task['id']
    logger = get_logger_adapter(tid)
    logger.info("Handle task")
    cfg = task['config']
    klass_name = cfg['class']
    cfg['logger'] = logger
    # Replace this name
    payload = task['token']
    try:
        result = _aggregate_host(klass_name, payload, cfg, task)
        response.write(msgpack.packb(result))
        logger.info("Done")
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
    raw = yield request.read()
    tid, cfg, data = msgpack.unpackb(raw)
    logger = get_logger_adapter(tid)
    logger.debug("Unpack raw data successfully")
    payload = map(msgpack.unpackb, data)
    klass_name = cfg['class']
    cfg['logger'] = logger
    try:
        result = _aggregate_group(klass_name, payload, cfg)
    except KeyError:
        response.error(-100, "There's no class named %s" % klass_name)
        logger.error("class %s is absent", klass_name)
    except Exception as err:  # pylint: disable=broad-except
        logger.error("%s", err)
        response.error(100, repr(err))
    else:
        logger.info("Result of group aggreagtion %s", str(result))
        response.write(result)
        response.close()


def _aggregate_host(klass_name, payload, config, task):
    available = plugin_import()
    klass = available[klass_name]
    handler = klass(config)
    prevtime, currtime = task["prevtime"], task["currtime"]
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
