FROM ubuntu:20.10
RUN apt update && DEBIAN_FRONTEND=noninteractive apt full-upgrade -y && \
    DEBIAN_FRONTEND=noninteractive apt install -y --force-yes --no-install-recommends \
    build-essential python3-pip libcap-dev gcc g++ python3-dev libssl-dev wget locales \
    sudo less psmisc vim htop subversion openssh-client logrotate mawk \
    bind9-host unbound lsof jq zstd jnettop util-linux strace tcpdump \
    htop curl moreutils iptables iputils-tracepath util-linux \
    git iputils-ping netcat-openbsd iproute2 sysstat traceroute jnettop \
    dstat mtr-tiny tzdata libjemalloc2 \
    && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 10

RUN python3 -m pip install --no-cache-dir -U pip setuptools
RUN python3 -m pip install --no-cache-dir -U Cython python-prctl
RUN python3 -m pip install --no-cache-dir -U msgpack ujson PyYAML requests ps_mem

RUN wget -O /usr/bin/combaine-client  https://github.com/combaine/combaine-client/releases/download/v0.0.1/combaine-client-static-linux-amd64
RUN wget -O /usr/bin/ttail https://github.com/sakateka/ttail/releases/download/v0.0.2/ttail-static-linux-amd64

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
COPY build/juggler                 /usr/lib/combaine/apps/
RUN python3 -m pip install --no-cache-dir -U grpcio grpcio_tools
