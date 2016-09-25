#!/usr/bin/env python
"""
Multimetrics parse data like 'metric_name value' from hosts loggiver, and
aggregate result of hosts parsing via aggregate_group
"""

if __name__ == '__main__':
    import logging
    logging.basicConfig()
    get_logger_adapter = logging.getLogger  # pylint: disable=invalid-name
else:
    from combaine.common.logger import get_logger_adapter  # pylint: disable=import-error

DEFAULT_QUANTILE_VALUES = [75, 90, 93, 94, 95, 96, 97, 98, 99]


def _clean_timings(timings_str):
    return timings_str.replace(',', ' ').replace('- ', ' ').replace(':', ' ')


def _check_name(name):
    if '<' in name or '>' in name or ';' in name:
        raise NameError("Name of metric contains forbidden symbols: '<>;'")
    if '[' in name or ']' in name or '\\' in name or '/' in name:
        raise NameError("Name of metric contains forbidden symbols: '[]\\/'")


class Multimetrics(object):
    """
    General metrics count:
    ping.5xx 12
    api.2xx 3000
    traffix 234324234

    Special aggregator of prehandled quantile data
    Data looks like:
    uploader_timings_request_post_patch-url 0.001 0.001 0.002
    uploader_timings_request_post_upload-from-service
    uploader_timings_request_post_upload-url 0.001 0.002 0.001 0.002 0.001
    uploader_timings_request_put_patch-target 0.651 0.562 1.171

    or timings in packed form:
    metric name should startwith @
    @uploader_timings_request_post_upload-url 0.001@300 0.002@3 0.001@12
    """
    def __init__(self, config):
        self.quantile = list(config.get("values", DEFAULT_QUANTILE_VALUES))
        self.quantile.sort()
        # recalculate to rps? default yes
        self.rps = (config.get("rps", "yes") == "yes")
        # find timings by specified string. default '_timings'
        self.timings_is = config.get("timings_is", "_timings")
        # multiply on factor: default `1`
        factor = float(config.get("factor", 1))
        self.factor = lambda item: factor * item
        # get prc. By default - No. Format: { ext_services: error/info }
        # get prc of errors in info from metrics which contents 'ext_services'
        self.get_prc = config.get("get_prc", False)

        self.log = config.get("logger", get_logger_adapter("Multimetrics"))

    def is_timings(self, name):
        "Check metric name against is timings"
        return self.timings_is in name

    def add_timings(self, container, name, timings_value, packed=False):
        """
        Pack comming timings in to compact dict where
        keys are timings, and values are count of timings
        """
        try:
            tim_dict = container[name]
        except KeyError:
            container[name] = {}
            tim_dict = container[name]

        if packed:
            timings_value, count = timings_value.split("@")
            count = float(count)
        else:
            count = 1.0

        key = float(timings_value)
        try:
            tim_dict[key] += count
        except KeyError:
            tim_dict[key] = count

    def aggregate_host(self, payload, prevtime, currtime):
        """ Convert strings of 'payload' into dict[string][]float and return """
        add_timings = self.add_timings

        delta = float(currtime - prevtime)
        if delta <= 0:
            delta = 1.0

        result = {}
        for line in payload.splitlines():
            line = line.strip()
            if not line:
                continue

            name, _, metrics_as_strings = line.partition(" ")
            try:
                name.decode('ascii')
                _check_name(name)

                if self.is_timings(name):
                    if name[0] == '@':
                        name = name[1:]  # make timings name valid
                        timings_is_packed = True
                    else:
                        timings_is_packed = False

                    metrics_as_strings = _clean_timings(metrics_as_strings)

                    if name not in result:
                        result[name] = {}

                    for tmn in metrics_as_strings.split():
                        add_timings(result, name, tmn, timings_is_packed)
                else:
                    metrics_as_values = sum(map(float, metrics_as_strings.split()))
                    if self.rps:
                        metrics_as_values /= delta

                    try:
                        result[name] += metrics_as_values
                    except KeyError:
                        result[name] = metrics_as_values
            except Exception as err:  # pylint: disable=broad-except
                self.log.error("Unable to parse %s: %s", line, err)
        return result

    def aggregate_group(self, payload):
        """ Payload is list of dict[string][]float"""
        if len(payload) == 0:
            raise Exception("No data to aggregate")
        names_of_metrics = set()
        map(names_of_metrics.update, (i.keys() for i in payload))
        result = {}

        for metric in names_of_metrics:
            if self.is_timings(metric):
                agg_timings = {}
                count = 0.0
                for item in payload:
                    for tmk, v in item.get(metric, {}).items():
                        try:
                            agg_timings[tmk] += v
                        except:
                            agg_timings[tmk] = v
                        count += v

                if len(agg_timings) == 0:
                    continue

                keys = sorted(agg_timings.keys())
                value = keys[0]

                result[metric] = list()
                for q in self.quantile:
                    if q < 100:
                        index = int(count / 100 * q)
                        sumidx = 0
                        for i in keys:
                            sumidx += agg_timings[i]
                            if index <= sumidx:
                                value = i
                                break
                    else:
                        value = keys[-1]

                    result[metric].append(self.factor(value))
            else:
                metric_sum = sum(item.get(metric, 0) for item in payload)
                result[metric] = metric_sum

        if self.get_prc:
            for field in self.get_prc:
                prc_result = {}
                p, a = self.get_prc[field].split('/')
                for k in result:
                    if field in k:
                        mp = '.'.join(k.split('.')[:-1])
                        mp_a = '.'.join((mp, a))
                        mp_p = '.'.join((mp, p))
                        mp_err = mp + '.err_prc'
                        if mp_err not in result:
                            if result[mp_a]:
                                if result[mp_p]:
                                    if result[mp_a] < result[mp_p]:
                                        prc_result[mp_err] = 100
                                    else:
                                        prc_result[mp_err] = round(
                                            result[mp_p] * 100.0 / result[mp_a], 2)
                                else:
                                    prc_result[mp_err] = 0
                            elif result[mp_p]:
                                prc_result[mp_err] = 100
                result.update(prc_result)

        return result


def test(datafile):
    """Simple test with time measurement"""
    import time
    from pprint import pprint

    mms = Multimetrics({})
    print("+++ {} +++".format(mms.__dict__))
    with open(datafile, 'r') as fname:
        _payload = fname.read()
    start = time.time()
    res = mms.aggregate_host(_payload, 1, 3)
    print("+++ Aggregate host ({} s) +++".format(time.time() - start))
    _payload = [res, res, res, res, res, res, res, res, res, res, res, res, res]

    start = time.time()
    res = mms.aggregate_group(_payload)
    print("+++ Aggregate group ({} s) +++".format(time.time() - start))
    pprint(res, indent=1, width=120)

if __name__ == '__main__':
    import sys
    test(sys.argv[1])
