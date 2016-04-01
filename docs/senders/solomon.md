## Common Blocking Base

### Host configuration

API url to send data can be overridden in the configuration file ```/etc/combaine/solomon-api.conf```:

```
http://api.domain.net/api/push/json
```

### Senders section

```yaml
senders:
  app:
    # required
    type:       "solomon"
    project:    "name"
    # optional
    api:        "http://api.domain.net/api/push"
    cluster:    "clusterName"
    Fields:     ["25_prc", "50_prc", "99_prc"] # timings
```

### Description

Sample config

```yaml
---
parsing:
  groups: [group-of-hosts]
  metahost: host.example.com
  parser: NullParser
  raw: true
  agg_configs: [solomon]
  DataFetcher:
    timetail_url: "/task_data_fetch&log_ts="
  Combainer:
      MINIMUM_PERIOD: 20
aggregate:
  data:
    solomon:
      type: custom
      class: Multimetrics
  senders:
    solomon_dev_sender:
      type: solomon
      project: megaproject
      cluster: megadev
```
