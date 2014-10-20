
import json

from combaine.common import ParsingTask


FIXTURE_PATH = "tests/fixtures/"


def test_parsing_interface():
    with open(FIXTURE_PATH + 'fixture_msgpack_parsing_task') as f:
        pt = ParsingTask(f.read())

    with open(FIXTURE_PATH + '/fixture_json_parsing_task') as f:
        etalon = json.load(f)

    assert pt.Host() == etalon["Host"], pt.Host()
