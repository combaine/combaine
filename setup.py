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

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

VERSION = "0.6"

setup(
    name="Combaine",
    version=VERSION,
    author="Anton Tyurin",
    author_email="noxiouz@yandex.ru",
    description="Distributed fault-tolerant system of\
                 data processing based on Cocaine\
                 (https://github.com/cocaine)",
    url="https://github.com/noxiouz/Combaine",
    license="LGPL3",
    packages=[
        'combaine',
        'combaine.combained',
        'combaine.combainer',
        'combaine.plugins',
        'combaine.utils'
    ]
)
