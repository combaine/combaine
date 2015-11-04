package cbb

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/noxiouz/Combaine/common/httpclient"
	"github.com/noxiouz/Combaine/common/logger"
	"github.com/noxiouz/Combaine/common/tasks"
)

const (
	CONNECTION_TIMEOUT = 2000 // ms
	RW_TIMEOUT         = 3000 // ms
)

var (
	CBBHTTPClient = httpclient.NewClientWithTimeout(
		time.Millisecond*CONNECTION_TIMEOUT,
		time.Millisecond*RW_TIMEOUT)
)

type CBBConfig struct {
	Items       []string `codec:"items"`
	Flag        int      `codec:"flag"`
	Host        string   `codec:"host"`
	TableType   int      `codec:"tabletype"`
	ExpireTime  int64    `codec:"expiretime"`
	Description string   `codec:"description"`
	Path        string   `codec:"path"`
}

type CBBSender struct {
	*CBBConfig
	id string
}

func NewCBBClient(cfg *CBBConfig, id string) (*CBBSender, error) {
	return &CBBSender{
		CBBConfig: cfg,
		id:        id,
	}, nil
}

func (c *CBBSender) makeBaseUrl() url.URL {
	if c.Path == "" {
		if c.TableType == 2 {
			c.Path = "/cgi-bin/set_netblock.pl"
		} else {
			c.Path = "/cgi-bin/set_range.pl"
		}
	}
	reqUrl := url.URL{
		Scheme: "http",
		Host:   c.Host,
		Path:   c.Path,
	}
	return reqUrl
}

func (c *CBBSender) makeUrlValues(ip string, code string, desc string) url.Values {
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

func (c *CBBSender) send(data tasks.DataType, timestamp uint64) ([]url.URL, error) {
	logger.Debugf("%s Data to send: %v", c.id, data)
	result := make([]url.URL, 0)
	for _, aggname := range c.Items {
		request := c.makeBaseUrl()
		var root, metricname string
		if items := strings.SplitN(aggname, ".", 2); len(items) > 1 {
			root, metricname = items[0], items[1]
		} else {
			root = items[0]
		}

		for _, value := range data[root] {
			subgroup := reflect.ValueOf(value)
			if subgroup.Kind() != reflect.Map {
				continue
			} // {4xx: {ip: "some text desc 99%"} ...

			codes := subgroup.MapIndex(reflect.ValueOf(metricname))
			if !codes.IsValid() {
				continue
			}
			ips := reflect.ValueOf(codes.Interface())
			if ips.Kind() != reflect.Map {
				continue
			}

			var desc string
			for _, ip := range ips.MapKeys() {
				desc_v := reflect.ValueOf(ips.MapIndex(ip).Interface())
				desc_t := desc_v.Type()

				if desc_v.Kind() == reflect.Slice && desc_t.Elem().Kind() == reflect.Uint8 {
					desc = string(desc_v.Bytes())
				} else {
					desc = fmt.Sprintf("%v", desc_v.Interface())
				}

				val := c.makeUrlValues(reflect.ValueOf(ip.Interface()).String(), metricname, desc)
				request.RawQuery = val.Encode()
				result = append(result, request)
			}
		}
	}
	return result, nil
}

func (c *CBBSender) Send(data tasks.DataType, timestamp uint64) error {
	result, err := c.send(data, timestamp)
	if err != nil {
		return err
	}
	for _, query := range result {
		req, err := http.NewRequest("GET", query.String(), nil)
		if err != nil {
			return err
		}
		resp, err := CBBHTTPClient.Do(req)
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
