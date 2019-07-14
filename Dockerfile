FROM ubuntu:disco
RUN apt update \
    && apt install build-essential -y --force-yes python3-pip
RUN python3 -m pip install grpcio --no-binary grpcio
RUN python3 -m pip install grpcio_tools
RUN python3 -m pip install msgpack
RUN python3 -m pip install ujson
RUN python3 -m pip install Cython
RUN apt install libcap-dev -y --force-yes
RUN python3 -m pip install python-prctl

COPY build/combainer               /usr/bin/
COPY build/worker                  /usr/bin/combaine-worker

COPY aggregator/                   /usr/lib/combaine/apps/aggregator/
COPY build/graphite                /usr/lib/combaine/apps/
COPY build/solomon                 /usr/lib/combaine/apps/
COPY build/juggler                 /usr/lib/combaine/apps/
COPY plugins/aggregators/          /usr/lib/combaine/custom
RUN for f in /usr/lib/combaine/custom/*.py; do python3 /usr/local/bin/cythonize -3 -i $f; done
RUN mkdir -p /etc/combaine /var/log/combaine
RUN touch /etc/combaine/juggler.yaml /etc/combaine/combaine.yaml
