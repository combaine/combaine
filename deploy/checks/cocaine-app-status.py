#!/usr/bin/env python
# pylint: disable=wrong-import-position, invalid-name, bare-except
"""Cocaine app status check"""

# Provides: cocaine-app

import sys
sys.stderr = open("/dev/null")
from cocaine.services import Service
from cocaine.asio.exceptions import LocatorResolveError, ConnectionError

# ./cocaine-app-status.py graphite juggler agave solomon razladki custom
ERRS = []
STATUS = 0
MSG = "OK"

try:
    for args in sys.argv[1:]:
        try:
            i = Service(args).info().get()  # pylint: disable=no-member
        except LocatorResolveError:
            ERRS.append("%s: app: not running" % args)
        except ConnectionError:
            ERRS.append("%s: app: not running" % args)
        try:
            depth = i["queue"]["depth"]
            capacity = i["queue"]["capacity"]
            state = i["state"]
        except KeyError:
            ERRS.append("%s: app: not running" % args)
        except NameError:
            pass
        else:
            if state == "broken":
                ERRS.append("%s app: broken" % args)
            elif float(depth) / capacity > 0.9:
                ERRS.append("%s app: queue is full" % args)

    if ERRS:
        STATUS = 2
        MSG = str(ERRS)
except:
    STATUS = 2
    MSG = "{0[0].__name__}: {0[1]}!".format(sys.exc_info())

print "PASSIVE-CHECK:coccaine-app;{};{}".format(STATUS, MSG)
