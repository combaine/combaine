#!/usr/bin/env python
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

import cPickle
import uuid
from warnings import filterwarnings
import yaml

import msgpack
import MySQLdb

from cocaine.worker import Worker
from cocaine.logging import Logger

from combaine.cloud.dg import MySqlDG

# Drop warnings
filterwarnings('ignore', category=MySQLdb.Warning)
log = Logger()

CONFIG_PATH = "/etc/combaine/mysql.yaml"

with open(CONFIG_PATH, 'r') as f:
    mysql_config = yaml.load(f)


mysqldg = MySqlDG(log, **mysql_config)


def put(request, response):
    raw = yield request.read()
    data = msgpack.unpackb(raw)
    tablename = "CMB" + str(uuid.uuid4()).replace("-", "")[:20]
    log.debug("Put data into %s" % tablename)
    try:
        mysqldg.putData(data, tablename)
    except Exception as err:
        response.error(-100, str(err))
    else:
        response.write(tablename)
        response.close()


def drop(request, response):
    raw = yield request.read()
    tablename = msgpack.unpackb(raw)
    try:
        drop_query = "DROP TABLE IF EXISTS %s" % tablename
        log.debug(drop_query)
        mysqldg.perfomCustomQuery(drop_query)
    except Exception as err:
        response.error(-100, str(err))
    else:
        response.write("ok")
        response.close()


def query(request, response):
    raw = yield request.read()
    tablename, querystr = msgpack.unpackb(raw)
    try:
        log.debug("QUERY STRING: %s " % querystr)
        res = cPickle.dumps(mysqldg.perfomCustomQuery(querystr))
    except Exception as err:
        response.error(-99, str(err))
    else:
        response.write(res)
        response.close()

if __name__ == "__main__":
    W = Worker()
    W.run({"put": put,
           "drop": drop,
           "query": query})
