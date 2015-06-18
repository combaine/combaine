## juggler

### Hosts configuration

A configuration file must be in ```/etc/combaine/juggler.yaml```
to specify juggler hosts:

```yaml
juggler_hosts: ["host1:8998", "host2:8998"]
juggler_frontend: ["front1:8998", "front2:8998"]
```

### Senders section

```yaml
type: "juggler"
checkname: "testcheck"
description: "very good description for the check"  # optional
flap:
  - "flap_time": 0  # optional
  - "stable_time": 600  # optional
  - "critical_time": 1200  # optional
host: "hostname_for_jugger_check"
method: "GOLEM,SMS"
aggregator_kwargs:
  - "ignore_nodata": 1
variables:
  - "var1": "100 + 200"
  - "load": "iftimeofday(20, 6, 100, 200)"  # load = 10 from 20:00 to 6:00, otherwise 200
# conditions for different levels
crit: ["$nginx['20x'] < load"]
warn: ["$nginx['20x'] < load + 50"]
info: []
ok: []
```
