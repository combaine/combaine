FROM ubuntu:trusty

COPY build/combainer               /usr/bin/
COPY build/worker                  /usr/bin/combaine-worker

COPY deploy                        /usr/lib/combaine
COPY plugins/aggregators/custom.py /usr/lib/combaine/apps/
COPY build/agave                   /usr/lib/combaine/apps/
COPY build/graphite                /usr/lib/combaine/apps/
COPY build/razladki                /usr/lib/combaine/apps/
COPY build/cbb                     /usr/lib/combaine/apps/
COPY build/solomon                 /usr/lib/combaine/apps/
COPY build/juggler                 /usr/lib/combaine/apps/
COPY build/monder                  /usr/lib/combaine/apps/
