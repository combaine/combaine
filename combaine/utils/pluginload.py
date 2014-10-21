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

import imp
import os


class UnavailablePluginError(Exception):
    pass


class Plugins(object):
    _EXTS = [ext[0] for ext in imp.get_suffixes()]

    def __init__(self, path, extra_filter=None):
        self.path = path  # search path for pluigns
        self.available = None
        self.extra_filter = extra_filter

    def get_plugin(self, name):
        if self.available is None:
            self.reload()

        try:
            return self.available[name]
        except KeyError as err:
            raise UnavailablePluginError("plugin %s is unavailable" % err)

    def reload(self):
        self.available = self._load_plugins()

    def _is_plugin(self, candidate):
        name, extension = os.path.splitext(candidate)
        return extension in self._EXTS and name != "__init__"

    def _load_plugins(self):
        modules = set(map(lambda x: x.split('.')[0],
                          filter(self._is_plugin, os.listdir(self.path))))
        all_plugins = {}
        for module in modules:
            try:
                fp, path, descr = imp.find_module(module, [self.path])
            except ImportError:
                continue

            try:
                _temp = imp.load_module("temp", fp, path, descr)
                for item in filter(lambda x: not x.startswith("__"),
                                   dir(_temp)):
                    candidate = getattr(_temp, item)
                    if not self.extra_filter or self.extra_filter(candidate):
                        all_plugins[item] = candidate
            except (ImportError, Exception):
                pass
            finally:
                if fp:
                    fp.close()
        return all_plugins
