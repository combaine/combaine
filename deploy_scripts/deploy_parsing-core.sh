#!/bin/bash

APPNAME=parsing
MANIFEST=manifest_parsing-core.json
PACKAGE=parsing.tar.gz

rm ./$PACKAGE || true
tar -czf ./$PACKAGE ./*

cocaine-tool app upload --name $APPNAME --package=$PACKAGE --manifest=$MANIFEST
cocaine-tool runlist add-app --name combaine --app $APPNAME --profile default
