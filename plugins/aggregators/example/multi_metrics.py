#!/usr/bin/env python


DEFAULT_QUANTILE_VALUES = [75, 90, 93, 94, 95, 96, 97, 98, 99]
DEFAULT_TIMINGS_VALUE = [0.]
DEFAULT_VALUE = 0.


def is_timings(name):
    return "_timings" in name


class Multimetrics(object):
    """
    Special aggregator of prehandled quantile data
    Data looks like:
    uploader_timings_request_post_patch-url 0.001 0.001 0.002
    uploader_timings_request_post_upload-from-service
    uploader_timings_request_post_upload-url 0.001 0.002 0.001 0.002 0.001
    uploader_timings_request_put_patch-target 0.651 0.562 1.171
    """
    def __init__(self, config):
        self.quantile = config.get("values") or DEFAULT_QUANTILE_VALUES
        self.rps = ("yes" == config.get("rps", "yes"))
        factor = config.get("factor", 1)
        if factor == 1:
            self.factor = float
        else:
            self.factor = lambda x: factor * float(x)
        self.quantile.sort()

    def _parse_metrics(self, lines):
        factor = self.factor
        result = {}
        for line in lines:
            name, _, metrics_as_strings = line.partition(" ")
            if is_timings(name):
                # put a default placeholder here if there's no such result yet
                if not metrics_as_strings and name not in result:
                    result[name] = DEFAULT_TIMINGS_VALUE
                    continue
                try:
                    metrics_as_values = map(factor, metrics_as_strings.split())
                    if name in result:
                        result[name] += metrics_as_values
                    else:
                        result[name] = metrics_as_values
                except ValueError as err:
                    raise Exception("Unable to parse %s: %s" % (line, err))

            else:
                # put a default placeholder here if there's no such result yet
                if not metrics_as_strings and name not in result:
                    result[name] = DEFAULT_VALUE
                    continue
                try:
                    metrics_as_values = sum(map(float, metrics_as_strings.split()))
                    if name in result:
                        result[name] += metrics_as_values
                    else:
                        result[name] = metrics_as_values
                except ValueError as err:
                    raise Exception("Unable to parse %s: %s" % (line, err))
        return result

    def aggregate_host(self, payload, prevtime, currtime):
        """ Convert strings of payload into dict[string][]float and return """
        result = self._parse_metrics(payload.splitlines())
        if self.rps:
            delta = float(currtime - prevtime)
            if delta <= 0:
                delta = 1
            for name in (key for key in result.keys() if not is_timings(key)):
                result[name] /= delta
        return result

    def aggregate_group(self, payload):
        """ Payload is list of dict[string][]float"""
        if len(payload) == 0:
            raise Exception("No data to aggregate")
        names_of_metrics = set()
        map(names_of_metrics.update, (i.keys() for i in payload))
        result = {}
        for metric in names_of_metrics:
            if is_timings(metric):
                result[metric] = list()
                all_resuts = list()
                for item in payload:
                    all_resuts.extend(item.get(metric, []))

                if len(all_resuts) == 0:
                    continue

                all_resuts.sort()
                count = float(len(all_resuts))
                for q in self.quantile:
                    if q < 100:
                        index = int(count / 100 * q)
                    else:
                        index = count - 1
                    result[metric].append(all_resuts[index])
            else:
                result[metric] = sum(item.get(metric, 0) for item in payload)

        return result

if __name__ == '__main__':
    import pprint
    m = Multimetrics({})
    with open('example/t.log', 'r') as f:
        payload = f.read()
    r = m.aggregate_host(payload, None, None)
    pprint.pprint(r)
    payload = [r, r, r]

    pprint.pprint(m.aggregate_group(payload))
