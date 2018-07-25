#!/bin/bash

WD=/usr/lib/combaine/apps
pushd $WD

rm -vf ./*.tar.gz ./*.json
for app in $(ls -1); do
    name=${app%.*}
    package=$name.tar.gz
    manifest=$name.json

    echo '{"slave": "'$app'"}' > $manifest
    tar -czf ./$package ./$name*

    cocaine-tool app upload --name $name --package=$package --manifest=$manifest
    cocaine-tool runlist add-app --name combaine --app $name --profile default
    rm -vf ./*.tar.gz ./*.json
done
