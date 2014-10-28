#!/usr/bin/env python

import os
import sys
import imp

import msgpack

from cocaine.worker import Worker
from cocaine.logging import Logger

Log = Logger()


PATH = os.environ.get('PLUGINS_PATH',
                      '/usr/lib/yandex/combaine/custom')
sys.path.insert(0, PATH)

EXTS = [ext for (ext, _, _) in imp.get_suffixes()]


def _isPlugin(candidate):
    name, extension = os.path.splitext(candidate)
    if name != "__init__" and extension in EXTS:
        return True
    else:
        return False


def plugin_import():
    modules = set(map(lambda x: x.split('.')[0], filter(_isPlugin,
                                                        os.listdir(PATH))))
    all_parser_functions = {}
    for module in modules:
        try:
            fp, path, descr = imp.find_module(module, [PATH])
        except ImportError:
            continue
        else:
            try:
                _temp = imp.load_module("temp", fp, path, descr)
                for item in filter(lambda x: not x.startswith("__"),
                                   dir(_temp)):
                    candidate = getattr(_temp, item)
                    if callable(candidate):
                        all_parser_functions[item] = candidate
            except ImportError as err:
                Log.error("ImportError. Module: %s %s" % (module, repr(err)))
                pass
            except Exception as err:
                Log.error("Exception. Module: %s %s" % (module, repr(err)))
                pass
            finally:
                if fp:
                    fp.close()
    Log.debug("%s are available functions for parsing"
              % ' '.join(all_parser_functions.keys()))
    return all_parser_functions


def aggregate_host(request, response):
    raw = yield request.read()
    TASK = msgpack.unpackb(raw)
    tid = TASK['id']
    Log.info("%s Handle task" % tid)
    cfg = TASK['config']
    klass_name = cfg['class']
    # Replace this name
    payload = TASK['token']
    try:
        result = _aggregate_host(klass_name, payload, cfg, task)
        response.write(msgpack.packb(result))
        Log.info("%s Done" % tid)
    except KeyError:
        response.error(-100, "There's no class named %s" % klass_name)
        Log.error("%s class %s is absent" % (tid, klass_name))
    except Exception as err:
        response.error(-3, "Exception during handling %s" % repr(err))
        Log.error("%s Error" % tid)
    finally:
        response.close()


def aggregate_group(request, response):
    raw = yield request.read()
    cfg, data = msgpack.unpackb(raw)
    Log.debug("Unpack raw data successfully")
    payload = map(msgpack.unpackb, data)
    klass_name = cfg['class']
    try:
        result = _aggregate_group(klass_name, payload, cfg, task)
    except KeyError:
        response.error(-100, "There's no class named %s" % klass_name)
        Log.error("class %s is absent" % klass_name)
    except Exception as err:
        Log.error(str(err))
        response.error(100, repr(err))
    else:
        Log.info("Result of group aggreagtion " + str(result))
        response.write(result)
        response.close()


def _aggregate_host(klass_name, payload, config, task):
    available = plugin_import()
    klass = available[klass_name]
    handler = klass(config)
    prevtime, currtime = task["prevtime"], task["currtime"]
    return handler.aggregate_host(payload, prevtime, currtime)


def _aggregate_group(klass_name, payload, config, task):
    available = plugin_import()
    klass = available[klass_name]
    handler = klass(config)
    return handler.aggregate_group(payload)


if __name__ == '__main__':
    W = Worker()
    W.run({"aggregate_host": aggregate_host,
           "aggregate_group": aggregate_group})
