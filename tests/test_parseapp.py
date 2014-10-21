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

from combaine.cloud.parsingapp import apply_parse
from combaine.cloud.parsingapp import get_logger_adapter
from combaine.utils.pluginload import Plugins
from combaine.common import ParserTask


def test_apply_parse():
    pl = Plugins("tests/fixtures/dummy", callable)
    task = ParserTask(msgpack.packb(["myuniqtid", "good", "somedata"]))
    result = apply_parse(task, pl, get_logger_adapter(task.tid)).get()
    assert result == [[('A', 1), ('B', 2)]]
