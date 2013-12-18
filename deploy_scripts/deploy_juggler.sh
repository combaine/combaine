#!/bin/bash

APPNAME=juggler
MANIFEST=manifest_aggregate-quant.json
PACKAGE=juggler.tar.gz

rm ./$PACKAGE || true
tar -czf ./$PACKAGE ./*

cocaine-tool app upload --name $APPNAME --package=$PACKAGE --manifest=$MANIFEST
cocaine-tool runlist add-app --name combaine --app $APPNAME --profile default
