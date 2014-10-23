# -*- coding: utf-8 -*-
#
# Copyright (c) 2012+ Tyurin Anton noxiouz@yandex.ru
#
# This file is part of Combaine.
#
# Combaine is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# Combaine is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#


import os

import msgpack

from cocaine.futures import chain

from combaine.common.logger import get_logger_adapter
from combaine.utils.pluginload import UnavailablePluginError
from combaine.utils.pluginload import Plugins
from combaine.common import ParserTask


PATH = os.environ.get('PARSERS_PATH') or '/usr/lib/yandex/combaine/parsers'
PLUGINS = Plugins(PATH, get_logger_adapter("CORE"), callable)


@chain.concurrent
def do_parse(parser, data):
    return [i.items() for i in parser(data) if i is not None]


@chain.source
def apply_parse(task, plugins, log):
    """
    task - instance of ParserTask
    """
    log.info("start parsing of data")
    func = plugins.get_plugin(task.parser_name)
    yield do_parse(func, task.data)


def parse(request, response):
    inc = yield request.read()
    task = ParserTask(inc)
    logger = get_logger_adapter(task.tid)
    try:
        result = yield apply_parse(task, PLUGINS, logger)
        response.write(msgpack.packb(result))
        logger.info("%d items have been parsed by %s",
                    len(result), task.parser_name)
        logger.debug("%s", result)
        logger.info("the parsing of data is done")
    except UnavailablePluginError as err:
        response.error(-100, "UnavailableParser: %s" % err)
        logger.error("Parser %s is absent", task.parser_name)
    except Exception as err:
        response.error(-3, "Exception in parsing %s" % repr(err))
        logger.exception("Unknown error: %s", err)
    finally:
        response.close()
