#!/usr/bin/env python3
"""Aggregator with extensions support"""

import contextlib
import importlib
import logging
import multiprocessing
import os
import signal
import socket
import sys
import time
from concurrent import futures
from logging.handlers import RotatingFileHandler

import grpc
import msgpack

import aggregator_pb2
import aggregator_pb2_grpc
import prctl

_PROCESS_COUNT = 4


class RidAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return '%s.%s %s' % (self.extra['rid'], self.extra['klass'], msg), kwargs


class Aggregator(aggregator_pb2_grpc.AggregatorServicer):
    """Combaine aggregator custom plugin loader"""

    def __init__(self):
        self.log = logging.getLogger("combaine")

        self.path = os.environ.get('PLUGINS_PATH', '/usr/lib/combaine/custom')
        self.all_custom_parsers = self.load_plugins()

    def load_plugins(self):
        parsers = {}
        names = set(c.split('.')[0] for c in os.listdir(self.path) if self._is_plugin(c))
        for name in names:
            plugin_file = self.get_plugin_file(name)
            if plugin_file is None:
                self.log.debug("load_plugins skip: %s", name)
                continue

            try:
                spec = importlib.util.spec_from_file_location(name, plugin_file)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                self.log.debug("Import parsers from: %s", plugin_file)
                for item in (x for x in dir(module) if self._is_candidate(x)):
                    candidate = getattr(module, item)
                    if callable(candidate):
                        parsers[item] = candidate
            except Exception as err:
                self.log.error("ImportError. Module: %s %s", name, repr(err))

        self.log.info("%s are available custom plugin for parsing", parsers.keys())
        return parsers

    def get_plugin_file(self, name):
        file_base = os.path.join(self.path, name)
        for ext in importlib.machinery.EXTENSION_SUFFIXES:
            mod_name = file_base + ext
            if os.path.exists(mod_name):
                return mod_name
        # try compiled file
        mod_name = file_base + '.py'
        mod_cache = importlib.util.cache_from_source(mod_name)
        if os.path.exists(mod_cache):
            return mod_cache
        if os.path.exists(mod_name):
            return mod_name
        return None

    @staticmethod
    def _is_plugin(name):
        maybe = any(name.endswith(e) for e in importlib.machinery.all_suffixes())
        return not name.startswith("_") and maybe

    @staticmethod
    def _is_candidate(name):
        return not name.startswith("_") and name[0].isupper()

    def getClass(self, logger, name, context):
        klass = self.all_custom_parsers.get(name, None)
        if not klass:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            msg = "Class '{}' not found!".format(name)
            context.set_details(msg)
            logger.error(msg)
            raise NameError(msg)
        return klass

    def getConfig(self, request):
        cfg = msgpack.unpackb(request.task.config, raw=False)
        logger = RidAdapter(self.log, {'rid': request.task.id, "klass": request.class_name})
        cfg['logger'] = logger
        return cfg

    def Ping(self, request, context):
        return aggregator_pb2.PongResponse()

    def AggregateHost(self, request, context):
        """
        Gets the result of a single host,
        performs parsing and their aggregation
        """
        cfg = self.getConfig(request)
        logger = cfg['logger']

        klass = self.getClass(logger, request.class_name, context)

        prevtime = request.task.frame.previous
        currtime = request.task.frame.current
        hostname = request.task.meta.get("host")
        result = klass(cfg).aggregate_host(request.payload, prevtime, currtime, hostname)

        if cfg.get("logHostResult", False):
            logger.info("Aggregate host result %s: %s", request.task.meta, result)

        result_bytes = msgpack.packb(result)
        return aggregator_pb2.AggregateHostResponse(result=result_bytes)

    def AggregateGroup(self, request, context):
        """
        Receives a list of results from the aggregate_host,
        and performs aggregation by group
        """
        cfg = self.getConfig(request)
        logger = cfg['logger']
        klass = self.getClass(logger, request.class_name, context)
        payload = [msgpack.unpackb(i, raw=False) for i in request.payload]
        result = klass(cfg).aggregate_group(payload)

        if cfg.get("logGroupResult", False):
            logger.info("Aggregate group result %s: %s", request.task.meta, result)
        result_bytes = msgpack.packb(result)
        return aggregator_pb2.AggregateGroupResponse(result=result_bytes)


@contextlib.contextmanager
def _reserve_port():
    """Reserve a port for all subprocesses to use."""
    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    if sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT) != 1:
        raise RuntimeError("Failed to set SO_REUSEPORT.")
    sock.bind(('', 10000))
    try:
        yield sock.getsockname()[1]
    finally:
        sock.close()


def _run_server(bind_address):
    """Start a server in a subprocess."""
    prctl.set_pdeathsig(signal.SIGTERM)
    logging.info('Starting new server.')
    options = (
        ('grpc.so_reuseport', 1),
        ('grpc.max_send_message_length', 128 * 1024 * 1024),
        ('grpc.max_receive_message_length', 128 * 1024 * 1024),
    )

    # WARNING: This example takes advantage of SO_REUSEPORT. Due to the
    # limitations of manylinux1, none of our precompiled Linux wheels currently
    # support this option. (https://github.com/grpc/grpc/issues/18210). To take
    # advantage of this feature, install from source with
    # `pip install grpcio --no-binary grpcio`.

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=options)
    aggregator_pb2_grpc.add_AggregatorServicer_to_server(Aggregator(), server)
    server.add_insecure_port(bind_address)
    server.start()
    _wait_forever(server)


def _wait_forever(server):
    try:
        while True:
            time.sleep(60 * 60 * 24)
    except KeyboardInterrupt:
        server.stop(0)


def serve():
    with _reserve_port() as port:
        bind_address = '[::]:{}'.format(port)
        logging.info("Binding to '%s'", bind_address)
        workers = []
        for _ in range(_PROCESS_COUNT):
            # NOTE: It is imperative that the worker subprocesses be forked before
            # any gRPC servers start up. See
            # https://github.com/grpc/grpc/issues/16001 for more details.
            worker = multiprocessing.Process(target=_run_server, args=(bind_address, ))
            workers.append(worker)
        for worker in workers:
            worker.daemon = True
            worker.start()
        while True:
            for worker in workers:
                worker.join(1)
                if worker.exitcode:
                    logging.error("worker exit by %s", worker.exitcode)
                    raise RuntimeError("Some worker disappear")


if __name__ == '__main__':
    root = logging.getLogger()
    maxSize = 512 * 1024 * 1024  # 0.5 Gb
    h = logging.handlers.RotatingFileHandler('/var/log/combaine/aggregator.log', 'a', maxSize, 8)
    f = logging.Formatter('%(asctime)s %(levelname)5s: %(lineno)4s#%(funcName)-12s %(message)s')
    h.setFormatter(f)
    root.addHandler(h)
    logLevelName = "INFO"
    logLevel = logging.INFO
    try:
        name = sys.argv[sys.argv.index("-loglevel") + 1].upper()
        logLevel = getattr(logging, name)
        logLevelName = name
    except:  # noqa pylint: disable=E722
        pass
    logging.getLogger().setLevel(logLevel)
    logging.info("Current log level: %s", logLevelName)
    serve()
