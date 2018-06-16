#!/bin/bash

WD=/usr/lib/combaine/apps
pushd $WD

for app in $(ls -1); do
    package=$app.tar.gz
    manifest=$app.json

    rm -vf ./$package ./$manifest
    echo '{"slave": "'$app'"}' > $manifest
    tar -czf ./$package ./$app*

    cocaine-tool app upload --name $app --package=$package --manifest=$manifest
    cocaine-tool runlist add-app --name combaine --app $app --profile default

done
