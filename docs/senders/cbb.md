## Common Blocking Base

### Host configuration

To specify a host where the plugin sends data, configuration
must be placed here (by default) ```/etc/combaine/cbb.conf```:

```
cbb.domain.net
```
or
```
cbb.domain.net:8080
```

### Senders section

```yaml
senders:
 somesendername:
  # required
  type: "cbb"
  flag: 2                # project id
  items:
   - "2xx"
   - "4xx"
   - "5xx"
  # optional
  host: cbb.yandex.net   # override /etc/combaine/cbb.conf
  tabletype: 1           # ip table type (default 1)
  path: "/some/url/path" # override hardcoded table methods
  expiretime: 3600       # seconds while block expire (default never)
```

### Description

This plugin supports only values inside a map.
Let's have a look at a result:

```json
{
  "host1": {
    "2xx": {
      "10.20.21.126": 74.90206796028059
      }
  },
  "host": {
    "4xx": {
      "10.100.21.30": 74.75854383358097,
      "10.32.18.444": 73.74793615850301
    },
    "5xx": {
      "172.0.0.7": 70.77070119037275,
      "127.0.1.1": 72.78298485940877
    },
    "2xx": {
      "192.168.140.57": 72.52881101845779
      }
  },
  "host2": {
    "2xx": {
      "192.168.140.57": 72.52881101845779
    }
  }
}
```

To send data ```items``` and ```flag``` must be specified:

```yaml
  flag: 2
  items:
   - "5xx"
```

```2xx``` sends value from ```host.2xx```, ```host1.2xx``` and ```host2.2xx```
```5xx``` sends value from ```host.5xx```. Other fields will be ommited.

Result will be posted to http://cbb.domain.net as GET queries:
table type other than ```2```
```text
/some_path?description=Some+descriptoin&expire=1435380904&flag=2&operation=add&range_dst=10.12.10.6&range_src=10.12.10.6
```
table type ```2```
```text
/some_path?description=Some+description&expire=1435380904&flag=2&operation=add&net_ip=10.11.14.7&net_mask=32
```
```expire``` will be set as ```unix.Time.Now() + expiretime``` from sender config
Plugin does not support range or network mask other than ```/32```

```
```
