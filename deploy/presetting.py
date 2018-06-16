#!/usr/bin/env python
import os

from cocaine.tools.actions import runlist, profile
from cocaine.services import Service
from cocaine.exceptions import ChokeEvent

COMBAINE_RUNLIST = "combaine"

try:
    s = Service("storage")
    try:
        print("Update profile")
        profile_json = os.path.dirname(__file__) + "/profile.json"
        if not os.path.exists(profile_json):
            print("Use default profile")
            profile_json = os.path.dirname(__file__) + "/profile-default.json"
        profile.Upload(s, "default", profile_json).execute().get()
    except ChokeEvent:
        pass
    if COMBAINE_RUNLIST not in runlist.List(s).execute().get():
        try:
            print("Create empty runlist")
            runlist.Upload(
                s, COMBAINE_RUNLIST, os.path.dirname(__file__) + "/runlist.json"
            ).execute().get()
        except ChokeEvent:
            pass
except Exception as err:
    print('%s' % repr(err))
    exit(1)
