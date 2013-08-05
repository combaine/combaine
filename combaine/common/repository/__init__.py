# -*- coding: utf-8 -*-
#
# Copyright (c) 2012 Tyurin Anton noxiouz@yandex-team.ru
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


BASE_PATH = "/etc/combaine/"
VALID_CONFIG_EXTENSIONS = ('yaml', 'json')


class ConfigError(Exception):
    pass


class FormatError(ConfigError):

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return "Invalid file format %s. Use JSON or YAML" % self.msg


class MissingConfigError(ConfigError):

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return "Missing config file: %s.(%s)"\
            % (self.msg, '|'.join(VALID_CONFIG_EXTENSIONS))


class Repository(object):
    '''
    Provide an access to configuration files.
    '''
    def __init__(self, basepath=BASE_PATH):
        self._basepath = basepath

    def _listofconfigs(self, path):
        pass

    def _getconfig(self, path, name):
        pass

    def get_parsing_config(self, name):
        return self._getconfig(self._basepath + 'parsing/', name)

    def list_parsing_configs(self):
        return self._listofconfigs(self._basepath + 'parsing/')

    def get_aggregate_config(self, name):
        return self._getconfig(self._basepath + 'aggregate/', name)

    def list_aggregate_configs(self):
        return self._listofconfigs(self._basepath + 'aggregate/')
