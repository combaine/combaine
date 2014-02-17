#!/usr/bin/env python
import os
import imp

import msgpack

from cocaine.worker import Worker
from cocaine.logging import Logger

Log = Logger()


PATH = '/usr/lib/yandex/combaine/parsers'


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
            except ImportError:
                pass
            except Exception:
                pass
            finally:
                if fp:
                    fp.close()
    return all_parser_functions


def parse(request, response):
    inc = yield request.read()
    name, data = msgpack.unpackb(inc)
    available = plugin_import()
    func = available.get(name)
    if func is None:
        response.error(-2, "Missing function %s" % name)
        raise StopIteration
    try:
        response.write([i.items() for i in func(data.splitlines()) if i is not None])
    except Exception as err:
        response.error(-3, "Exception in parsing %s" % repr(err))
    else:
        response.close()


if __name__ == "__main__":
    W = Worker(disown_timeout=300)
    W.run({"parse": parse})
