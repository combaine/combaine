class Noxiouz(object):
    def __init__(self, config):
        pass

    def aggregate_host(self, payload, prevtime, currtime):
        return float(sum(payload))/(currtime - prevtime)

    def aggregate_group(self, payload):
        return sum(payload)
