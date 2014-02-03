#!/bin/bash

APPNAME=cfgmanager
MANIFEST=manifest_cfgmanager.json
PACKAGE=cfgmanager.tar.gz

rm ./$PACKAGE || true
tar -czf ./$PACKAGE ./*

cocaine-tool app upload --name $APPNAME --package=$PACKAGE --manifest=$MANIFEST
cocaine-tool runlist add-app --name combaine --app $APPNAME --profile default
