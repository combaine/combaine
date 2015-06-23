## Razladki

### Hosts configuration

To specify a host where the plugin sends data a configuration
must be placed here (by default) ```/etc/combaine/razladki.conf```:

```
razladkihost.domain.net
```

### Senders section

```yaml
senders:
 somesendername:
  type: "razladki"
  items:
   "nginx.20x": "a title of this metric"
   "nginx": "other title"
  project: "example_project_name"
```

### Description

This plugin supports only single values in the root level or inside a map.
Let's have a look at a result:

```json
{
	"othermetric": {
		"host1": 2000
	},
	"nginx": {
		"host1": [20, 30, 40], // unsupported
		"host2": {
			"20x": 1000,
			"40x": 1002,
		},
		"host3": 307,
		"host4": 408
	}
}
```

To send data ```items``` must be specified:

```yaml
items:
  - "nginx.20x": "a title of this metric"
  - "nginx": "other title"
```

```nginx.20x``` sends value from ```nginx.host2.20x```,
```nginx``` sends data from ```nginx.host3```, ```nginx.host4```. Other fields will be ommited.

Result looks like

```json
{
	"ts": 123,
	"params": {
		"host2_20x": {
			"value": "1000",
			"Meta": { "title": "a title of this metric" }
		},
		"host3_nginx": {
			"value": "307",
			"Meta": {"title": "other title"}
		},
		"host4_nginx": {
			"value": "408",
			"Meta": {"title": "other title"}
		}
	},
	"alarms": {
		"host2_20x": {
			"Meta": {"title": "other title"}
		},
		"host3_nginx": {
			"Meta": {"title": "other title"}
		},
		"host4_nginx": {
			"Meta": {"title": "other title"}
		}
	}
}
```

and is posted to:

```
http://razladkihost.domain.net/save_new_data_json/example_project_name
```
