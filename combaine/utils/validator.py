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

import logging
import types

from combaine.foreign.schema import Schema, And, Or, Optional


logger = logging.getLogger('combaine')


PARSING_SCHEMA = Schema({Optional("groups"): list,
                         "log_name": Or(str, unicode),
                         "agg_configs": And([str, unicode], len),
                         "parser": Or(str, unicode),
                         object: object })


class ValidationError(ValueError):

    def __init__(self, msg):
        super(ValidationError, self).__init__(self, msg)


def validateparsing(target):
    print target
    #logger.debug('Start validation')
    print PARSING_SCHEMA.validate(target)


if __name__ == "__main__":
    import json
    validateparsing(json.load(open('/etc/combaine/parsing/photo_proxy.json')))
