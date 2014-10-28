#!/usr/bin/env python

from custom import _aggregate_host
from custom import _aggregate_group


def test_main():
    cfg = {"class": "Noxiouz",
           "arg1": 1,
           "arg2": [1, 2, 3]}
    task = {"prevtime": 100,
            "currtime": 200}


    payload = [1, 2, 3, 4, 5, 6]
    klass_name = cfg["class"]
    result = _aggregate_host(klass_name, payload, cfg, task)
    assert result == 0.21, result
    group_result = _aggregate_group(klass_name, [result, result], cfg, task)
    assert group_result == 0.42, group_result

if __name__ == "__main__":
    test_main()
