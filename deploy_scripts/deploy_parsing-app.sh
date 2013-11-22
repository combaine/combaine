#!/bin/bash

APPNAME=parsingApp
MANIFEST=manifest_parsing-app.json
PACKAGE=parsing-app.tar.gz

rm ./$PACKAGE || true
tar -czf ./$PACKAGE ./*

cocaine-tool app upload --name $APPNAME --package=$PACKAGE --manifest=$MANIFEST
