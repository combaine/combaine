FROM ubuntu:disco
RUN apt update && apt install -y --force-yes --no-install-recommends \
    build-essential python3-pip libcap-dev \
    vim htop subversion openssh-client git psmisc \
    bind9-host unbound lsof jq zstd jnettop util-linux \
    strace tcpdump htop curl moreutils iptables \
    gcc python3-dev wget runit sudo less locales \
    mawk python2 \
    && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*


RUN python3 -m pip install --no-cache-dir -U pip Cython setuptools
RUN python3 -m pip install --no-cache-dir -U grpcio --no-binary grpcio
RUN python3 -m pip install --no-cache-dir -U grpcio_tools python-prctl
RUN python3 -m pip install --no-cache-dir -U msgpack ujson PyYAML requests

RUN wget -O /usr/bin/combaine-client  https://github.com/combaine/combaine-client/releases/download/v0.0.1/combaine-client-static-linux-amd64
RUN wget -O /usr/bin/ttail https://github.com/sakateka/ttail/releases/download/v0.0.2/ttail-static-linux-amd64

# basic configure
RUN ln -vsTf /bin/bash /bin/sh
RUN ln -vsTf /bin/bash /bin/dash

COPY plugins/aggregators/          /usr/lib/combaine/custom
RUN for f in /usr/lib/combaine/custom/*.py; do cythonize -3 -i $f; done
RUN mkdir -p /etc/combaine /var/log/combaine
RUN touch /etc/combaine/juggler.yaml /etc/combaine/combaine.yaml

COPY build/combainer               /usr/bin/
COPY build/worker                  /usr/bin/combaine-worker
RUN chmod -c +x /usr/bin/combaine* /usr/bin/ttail

COPY aggregator/                   /usr/lib/combaine/apps/aggregator/
COPY build/graphite                /usr/lib/combaine/apps/
COPY build/solomon                 /usr/lib/combaine/apps/
COPY build/juggler                 /usr/lib/combaine/apps/
