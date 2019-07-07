FROM ubuntu:trusty

COPY build/combainer               /usr/bin/
COPY aggregator/aggregator.py      /usr/bin/
COPY build/worker                  /usr/bin/combaine-worker

COPY deploy                        /usr/lib/combaine
COPY build/graphite                /usr/lib/combaine/apps/
COPY build/razladki                /usr/lib/combaine/apps/
COPY build/solomon                 /usr/lib/combaine/apps/
COPY build/juggler                 /usr/lib/combaine/apps/
