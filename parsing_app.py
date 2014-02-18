#!/usr/bin/env python
import os
import sys
import imp

import msgpack

from cocaine.worker import Worker
from cocaine.futures.chain import concurrent
from cocaine.logging import Logger

Log = Logger()


PATH = '/usr/lib/yandex/combaine/parsers'
sys.path.insert(0, PATH)


def _isPlugin(candidate):
    name, extension = os.path.splitext(candidate)
    if name != "__init__" and extension in (".py", ".pyc", ".pyo"):
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
            except Exception:
                Log.error("Exception. Module: %s %s" % (module, repr(err)))
            finally:
                if fp:
                    fp.close()
    Log.debug("%s are available functions for parsing"
              % ' '.join(all_parser_functions.keys()))
    return all_parser_functions


@concurrent
def do_parse(parser, data):
    return [i.items() for i in parser(data) if i is not None]


def parse(request, response):
    inc = yield request.read()
    tid, name, data = msgpack.unpackb(inc)
    Log.info("%s Start" % tid)
    available = plugin_import()
    try:
        func = available[name]
        result = yield do_parse(func, data)
        Log.info("%s %d items have been parsed by %s" % (tid,
                                                         len(result),
                                                         name))
        response.write(result)
        Log.info("%s Done" % tid)
    except KeyError:
        response.error(-100, "There's no function named %s" % name)
        Log.error("%s Parser %s is absent" % (tid, name))
    except Exception as err:
        response.error(-3, "Exception in parsing %s" % repr(err))
        Log.error("%s Error" % tid)
    finally:
        response.close()


if __name__ == "__main__":
    W = Worker(disown_timeout=300)
    W.run({"parse": parse})
