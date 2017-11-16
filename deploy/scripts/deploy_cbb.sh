#!/bin/bash

CWD=${BASH_SOURCE[0]%/*}
pushd $CWD
APPNAME=cbb
MANIFEST=manifest_cbb.json
PACKAGE=cbb.tar.gz

rm ./$PACKAGE || true
tar -czf ./$PACKAGE ./*

cocaine-tool app upload --name $APPNAME --package=$PACKAGE --manifest=$MANIFEST
cocaine-tool runlist add-app --name combaine --app $APPNAME --profile default
popd
