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

import msgpack


def map_constructor(cfg, klass):
    if cfg is not None:
        return dict((k, klass(v)) for k, v in cfg.items())
    else:
        return {}


class ParsingConfig(object):
    def __init__(self, cfg):
        self.cfg = cfg

    @property
    def metahost(self):
        return self.cfg["Metahost"]


class Item(object):
    def __init__(self, cfg):
        self.config = cfg

    @property
    def Type(self):
        return self.config["type"]


class SenderItem(Item):
    pass


class DataItem(Item):
    pass


class AggregationConfig(object):
    def __init__(self, cfg):
        self.cfg = cfg

    @property
    def senders(self):
        return map_constructor(self.cfg.get("Senders"), SenderItem)

    @property
    def data(self):
        return map_constructor(self.cfg.get("Data"), DataItem)


class AggregationTask(object):
    def __init__(self, packed_task):
        self.task = msgpack.unpackb(packed_task)

    @property
    def Id(self):
        return self.task["Id"]

    @property
    def parsing_config(self):
        pr = ParsingConfig(self.task["ParsingConfig"])
        return pr

    @property
    def aggregation_config(self):
        agg = AggregationConfig(self.task["AggregationConfig"])
        return agg


class ParsingTask(object):
    def __init__(self, packed_task):
        self.task = msgpack.unpackb(packed_task)

    def host(self):
        return self.task["Host"]

    def parsing_config_name(self):
        return self.task["ParsingConfigName"]

    def parsing_config(self):
        return ParsingConfig(self.task["ParsingConfig"])

    def aggregation_configs(self):
        agg_cfgs = self.task.get("AggregationConfigs")
        if agg_cfgs is None:
            return {}
        else:
            return map_constructor(agg_cfgs, AggregationConfig)
