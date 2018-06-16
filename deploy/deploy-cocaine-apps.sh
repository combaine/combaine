#!/bin/bash

WD=/usr/lib/combaine/apps
pushd $WD

rm -vf ./*.tar.gz ./*.json
for app in $(ls -1); do
    package=$app.tar.gz
    manifest=$app.json

    echo '{"slave": "'$app'"}' > $manifest
    tar -czf ./$package ./$app*

    cocaine-tool app upload --name $app --package=$package --manifest=$manifest
    cocaine-tool runlist add-app --name combaine --app $app --profile default
    rm -vf ./$package ./$manifest
done
