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
import random
import types

import MySQLdb


def reconnect(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except MySQLdb.MySQLError as err:
            self.logger.error('Error in MySQLdb: %s. Try again' % err)
            self.ping(True)
            return func(self, *args, **kwargs)


class MySqlDG(object):

    def __init__(self, logger, **config):
        self.logger = logger
        self.place = None
        self.tablename = ''
        try:
            unix_socket = config.get('MysqlSocket',
                                     "/var/run/mysqld/mysqld.sock")
            self.dbname = config.get('local_db_name', 'COMBAINE')
            self.user = config.get('user', 'root')
            self.password = config.get('password', "")
            self.db = MySQLdb.connect(unix_socket=unix_socket,
                                      user=self.user,
                                      passwd=self.password)
            self.cursor = self.db.cursor()
            self.cursor.execute('CREATE DATABASE IF NOT EXISTS %s' % self.dbname)
            self.db.commit()
            self.db.select_db(self.dbname)
        except Exception as err:
            self.logger.error('Error in init MySQLdb %s' % err)
            raise Exception

    def ping(self):
        self.db.ping(True)

    @reconnect
    def putData(self, data, tablename):
        try:
            tablename = tablename.replace('.', '_').replace('-', '_').replace('+', '_')
            line = None
            fname = '/dev/shm/%s-%i' % ('COMBAINE', random.randint(0, 65535))
            with open(fname, 'w') as table_file:
                for line in data:
                    table_file.write('GOPA'.join([str(x) for _, x in line]) + '\n')
                table_file.close()

                if not line:
                    self.logger.info("Data for mysql is missed")
                    os.remove(table_file.name)
                    return False

                self.logger.debug('Data has been written to a temporary file %s, size: %d bytes'
                                  % (table_file.name, os.lstat(table_file.name).st_size))

            if not self._preparePlace(line):
                self.logger.error('Unsupported field types. Look at preparePlace()')
                return False

            self.cursor.execute('DROP TABLE IF EXISTS %s' % tablename)
            query = "CREATE TABLE IF NOT EXISTS `%(tablename)s` %(struct)s ENGINE = MEMORY DATA DIRECTORY='/dev/shm/'" % {'tablename': tablename,
                                                                                                                        'struct': self.place}
            self.cursor.execute(query)
            self.db.commit()

            query = "LOAD DATA INFILE '%(filename)s' INTO TABLE `%(tablename)s` FIELDS TERMINATED BY 'GOPA'" % {'filename': table_file.name,
                                                                                                              'tablename': tablename}
            self.cursor.execute(query)
            self.db.commit()
            if os.path.isfile(table_file.name):
                os.remove(table_file.name)
        except Exception as err:
            self.logger.error('Error in putData %s' % err)
            if os.path.isfile(table_file.name):
                os.remove(table_file.name)
            return False
        else:
            self.tablename = tablename
            return True

    def _preparePlace(self, example):
        ftypes = {types.IntType: "BIGINT",
                  types.UnicodeType: "VARCHAR(200)",
                  types.StringType: "VARCHAR(200)",
                  types.FloatType: "DOUBLE"}
        try:
            self.place = '( %s )' % ','.join([" `%s` %s" % (field_name,
                                                            ftypes[type(field_type)])
                                             for field_name, field_type in example])
        except Exception as err:
            self.logger.error('Error in preparePlace() %s' % err)
            self.place = None
            return False
        else:
            return True

    @reconnect
    def perfomCustomQuery(self, query_string):
        self.logger.debug("Execute query: %s" % query_string)
        self.cursor.execute(query_string)
        _ret = self.cursor.fetchall()
        self.db.commit()
        return _ret
