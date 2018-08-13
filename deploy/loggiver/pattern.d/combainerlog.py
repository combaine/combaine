#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Simple combainerlog parser
"""

import sys
from collections import defaultdict


def main():
    "main"
    result = {
        "loglevels": defaultdict(int),
        "cocaineQueue": defaultdict(int),
    }

    for line in sys.stdin:
        line = line.split("\t", 2)
        if len(line) > 2:
            level = line[1]
            result["loglevels"][level] += 1

    for key, val in result.items():
        for item, ival in val.items():
            print("{0}.{1} {2}".format(key, item, ival))

if __name__ == '__main__':
    main()
