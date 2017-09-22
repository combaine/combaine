#!/usr/bin/env python
import os

from cocaine.tools.actions import runlist, profile
from cocaine.services import Service
from cocaine.exceptions import ChokeEvent

COMBAINE_RUNLIST = "combaine"

try:
    s = Service("storage")
    try:
        print("Create empty profile")
        profile.Upload(s, "default", os.path.dirname(__file__) + "/profile.json").execute().get()
    except ChokeEvent:
        pass
    if COMBAINE_RUNLIST not in runlist.List(s).execute().get():
        try:
            print("Create empty runlist")
            runlist.Upload(s, COMBAINE_RUNLIST, os.path.dirname(__file__) + "/runlist.json").execute().get()
        except ChokeEvent:
            pass
except Exception as err:
    print('%s' % repr(err))
    exit(1)
