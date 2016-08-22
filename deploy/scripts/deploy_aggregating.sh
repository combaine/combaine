#!/bin/bash

APPNAME=aggregating
MANIFEST=manifest_aggregating.json
PACKAGE=aggregating.tar.gz

rm ./$PACKAGE || true
tar -czf ./$PACKAGE ./*

cocaine-tool app upload --name $APPNAME --package=$PACKAGE --manifest=$MANIFEST
cocaine-tool runlist add-app --name combaine --app $APPNAME --profile default
