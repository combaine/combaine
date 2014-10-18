#!/usr/bin/env python

from custom import _aggregate_host
from custom import _aggregate_group


def test_main():
    cfg = {"class": "Noxiouz",
           "arg1": 1,
           "arg2": [1, 2, 3]}

    payload = [1, 2, 3, 4, 5, 6]
    klass_name = cfg["class"]
    result = _aggregate_host(klass_name, payload, cfg)
    assert result == sum(payload), result
    group_result = _aggregate_group(klass_name, [result, result], cfg)
    assert group_result == 2 * result, group_result

if __name__ == "__main__":
    test_main()
