FROM ubuntu:trusty

# basic configure
RUN ln -vsTf /bin/bash /bin/sh
RUN ln -vsTf /bin/bash /bin/dash

RUN echo -e \
    "deb http://mirror.yandex.ru/ubuntu/ trusty main restricted universe multiverse\n"\
    "deb http://mirror.yandex.ru/ubuntu/ trusty-updates main restricted universe multiverse\n"\
    "deb http://mirror.yandex.ru/ubuntu/ trusty-backports main restricted universe multiverse\n"\
    "deb http://mirror.yandex.ru/ubuntu/ trusty-security main restricted universe multiverse\n"\
    > /etc/apt/sources.list

RUN apt-get -qq update \
    && apt-get install -y --force-yes --no-install-recommends \
    libjemalloc1 unbound psmisc python-yaml jq=1.4-2.1~ubuntu14.04.1 lsof \
    jnettop util-linux strace tcpdump htop curl moreutils \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

RUN locale-gen en_US.utf8 ru_RU.utf8

COPY build/combainer /usr/bin/
COPY build/worker /usr/bin/combaine-worker

COPY deploy                        /usr/lib/combaine
COPY plugins/aggregators/custom.py /usr/lib/combaine/apps/
COPY build/agave                   /usr/lib/combaine/apps/
COPY build/graphite                /usr/lib/combaine/apps/
COPY build/razladki                /usr/lib/combaine/apps/
COPY build/cbb                     /usr/lib/combaine/apps/
COPY build/solomon                 /usr/lib/combaine/apps/
COPY build/juggler                 /usr/lib/combaine/apps/
