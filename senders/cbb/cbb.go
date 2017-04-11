package cbb

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/httpclient"
	"github.com/combaine/combaine/common/logger"
)

// Config contains aggregation config and local sender configs
type Config struct {
	Items       []string `codec:"items"`
	Flag        int      `codec:"flag"`
	Host        string   `codec:"host"`
	TableType   int      `codec:"tabletype"`
	ExpireTime  int64    `codec:"expiretime"`
	Description string   `codec:"description"`
	Path        string   `codec:"path"`
}

// Sender talk with cbb api and send info abount blocked ips
type Sender struct {
	*Config
	id string
}

// InitializeLogger create cocaine logger
func InitializeLogger() {
	failbackToLocal := false
	logger.MustCreateLogger(failbackToLocal)
}

// NewCBBClient return cbb client
func NewCBBClient(cfg *Config, id string) (*Sender, error) {
	return &Sender{
		Config: cfg,
		id:     id,
	}, nil
}

func (c *Sender) makeBaseURL() url.URL {
	if c.Path == "" {
		if c.TableType == 2 {
			c.Path = "/cgi-bin/set_netblock.pl"
		} else {
			c.Path = "/cgi-bin/set_range.pl"
		}
	}
	return url.URL{
		Scheme: "http",
		Host:   c.Host,
		Path:   c.Path,
	}
}

func (c *Sender) makeURLValues(ip string, code string, desc string) url.Values {
	val := url.Values{
		"flag":      []string{strconv.Itoa(c.Flag)},
		"operation": []string{"add"},
	}
	if c.ExpireTime != 0 {
		val.Add("expire", strconv.Itoa(int(time.Now().Unix()+c.ExpireTime)))
	}
	if c.TableType != 2 {
		val.Add("range_src", ip)
		val.Add("range_dst", val["range_src"][0])
	} else {
		val.Add("net_ip", ip)
		val.Add("net_mask", "32")
	}
	if c.Description != "" {
		desc = fmt.Sprintf("%s: %s", c.Description, desc)
	}
	val.Add("description", desc)
	return val
}

func (c *Sender) send(data []common.AggregationResult, timestamp uint64) ([]string, error) {
	logger.Debugf("%s Data to send: %v", c.id, data)
	var requests []string

	var queryItems = make(map[string][]string)
	for _, aggname := range c.Items {
		keys := strings.SplitN(aggname, ".", 2)
		if len(keys) > 1 {
			queryItems[keys[0]] = append(queryItems[keys[0]], keys[1])
		} else {
			if _, ok := queryItems[keys[0]]; !ok {
				queryItems[keys[0]] = []string{}
			}
		}
	}

	for _, item := range data {
		var root string
		var metricsName []string
		var ok bool

		req := c.makeBaseURL()

		if root, ok = item.Tags["aggregate"]; !ok {
			logger.Errf("%s Failed to get data tag 'aggregate', skip task: %v", c.id, item)
			continue
		}
		if metricsName, ok = queryItems[root]; !ok {
			logger.Debugf("%s %s not in Items, skip task: %v", c.id, root, item)
			continue
		}

		subgroup := reflect.ValueOf(item.Result)
		if subgroup.Kind() != reflect.Map {
			logger.Debugf("%s CBB support only maps as task data: %v", c.id, subgroup)
			continue
		} // {4xx: {ip: "some text desc 99%"} ...

		for _, name := range metricsName {
			codes := subgroup.MapIndex(reflect.ValueOf(name))
			if !codes.IsValid() {
				logger.Debugf("%s invalid submap for codes %s: %v", c.id, name, codes)
				continue
			}
			ips := reflect.ValueOf(codes.Interface())
			if ips.Kind() != reflect.Map {
				// ips -> {ip: "some text desc 99%"}
				logger.Debugf("%s CBB support only maps as ips data: %v", c.id, ips)
				continue
			}

			var desc string
			for _, ip := range ips.MapKeys() {
				descV := reflect.ValueOf(ips.MapIndex(ip).Interface())
				descT := descV.Type()

				if descV.Kind() == reflect.Slice && descT.Elem().Kind() == reflect.Uint8 {
					desc = string(descV.Bytes())
				} else {
					desc = fmt.Sprintf("%v", descV.Interface())
				}

				val := c.makeURLValues(reflect.ValueOf(ip.Interface()).String(), name, desc)
				req.RawQuery = val.Encode()
				url := req.String()
				logger.Debugf("%s CBB block request: %s", c.id, url)
				requests = append(requests, url)
			}
		}
	}
	return requests, nil
}

// Send send task to cbb api
func (c *Sender) Send(ctx context.Context, data []common.AggregationResult, timestamp uint64) error {
	requests, err := c.send(data, timestamp)
	if err != nil {
		return err
	}
	for _, query := range requests {
		resp, err := httpclient.Get(ctx, query)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("bad response code '%d': %s", resp.StatusCode, resp.Status)
			}
			return fmt.Errorf("bad response code '%d' '%s': %s", resp.StatusCode, resp.Status, b)
		}
	}

	return nil
}
