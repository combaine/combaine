#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib2
import json


def curl():
    api = "http://localhost:9000/"
    try:
        req = urllib2.urlopen(api)
        if req.code != 200:
            raise Exception("Not OK code from combaine api")
        response = req.read()
    except Exception:
        pass
    else:
        print_metrics(response)


def dump(path, obj, dumper):
    metric = ""
    try:
        keys = path.split(".", 1)
        if len(keys) > 1:
            if keys[0] == "*":
                res = []
                for k in obj:
                    res.append(dump(keys[1], obj[k], float))
                metric = dumper(res)
            else:
                metric = dump(keys[1], obj[keys[0]], dumper)
        else:
            metric = dumper(obj[path])
    except Exception as e:  # NOQA
        # print("<combainer_monitoring_failed_as>: {0}".format(str(e)))
        pass
    return metric


def add(name, value):
    if str(value):
        return "{0} {1}\n".format(name, value)
    else:
        return ""


def print_metrics(resp):
    status = ""
    try:
        stats = json.loads(resp)
        status += add("nofile", dump("Files.Open", stats, str))
        status += add("goroutines", dump("GoRoutines", stats, str))
        status += add("clients", dump("Clients", stats, len))
        status += add("aggregate.total", dump("Clients.*.AggregateTotal", stats, sum))
        status += add("aggregate.failed", dump("Clients.*.AggregateFailed", stats, sum))
        status += add("aggregate.success", dump("Clients.*.AggregateSuccess", stats, sum))
        status += add("parsing.total", dump("Clients.*.ParsingTotal", stats, sum))
        status += add("parsing.failed", dump("Clients.*.ParsingFailed", stats, sum))
        status += add("parsing.success", dump("Clients.*.ParsingSuccess", stats, sum))

    except Exception as e:  # NOQA
        status = "<combainer/monitoring/failed/as> {0}".format(str(e))

    print(status)


if __name__ == '__main__':
    curl()
