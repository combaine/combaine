#!/bin/bash

APPNAME=quant
MANIFEST=manifest_aggregate-quant.json
PACKAGE=quant.tar.gz

rm ./$PACKAGE || true
tar -czf ./$PACKAGE ./*

cocaine-tool app upload --name $APPNAME --package=$PACKAGE --manifest=$MANIFEST
cocaine-tool runlist add-app --name combaine --app $APPNAME --profile default
