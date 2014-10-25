
import json

from combaine.common import ParsingTask, AggregationTask


FIXTURE_PATH = "tests/fixtures/"


def test_parsing_interface():
    with open(FIXTURE_PATH + 'fixture_msgpack_parsing_task') as f:
        pt = ParsingTask(f.read())

    with open(FIXTURE_PATH + '/fixture_json_parsing_task') as f:
        etalon = json.load(f)

    assert pt.host() == etalon["Host"], pt.Host()


def test_aggregation_task():
    with open(FIXTURE_PATH + 'fixture_msgpack_aggregation_task') as f:
        aggt = AggregationTask(f.read())

    with open(FIXTURE_PATH + '/fixture_json_aggregation_task') as f:
        etalon = json.load(f)

    assert aggt.Id == etalon["Id"]
    assert aggt.parsing_config.metahost == etalon["ParsingConfig"]["Metahost"]

    etalon_items = etalon["AggregationConfig"]["Data"].items()
