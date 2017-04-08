package httpfetcher

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/mitchellh/mapstructure"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/httpclient"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/parsing"
)

func init() {
	parsing.Register("http", NewHttpFetcher)
}

const (
	DEFAULT_CONNECTION_TIMEOUT = 1000
	DEFAULT_RW_TIMEOUT         = 3000
)

var (
	CONNECTION_TIMEOUT = DEFAULT_CONNECTION_TIMEOUT * time.Millisecond
	RW_TIMEOUT         = DEFAULT_RW_TIMEOUT * time.Millisecond
)

var HttpClient = httpclient.NewClientWithTimeout(CONNECTION_TIMEOUT, RW_TIMEOUT)

type HttpFetcher struct {
	httpFetcherConfig
}

type httpFetcherConfig struct {
	Port        int    `mapstructure:"port"`
	Uri         string `mapstructure:"uri"`
	ConnTimeout int    `mapstructure:"connection_timeout"`
	ReadTimeout int    `mapstructure:"read_timeout"`
}

func NewHttpFetcher(cfg map[string]interface{}) (t parsing.Fetcher, err error) {
	var (
		config         httpFetcherConfig
		decoder_config = mapstructure.DecoderConfig{
			// To allow decoder parses []uint8 as string
			WeaklyTypedInput: true,
			Result:           &config,
		}
	)

	decoder, err := mapstructure.NewDecoder(&decoder_config)
	if err != nil {
		return nil, err
	}

	err = decoder.Decode(cfg)
	if err != nil {
		return nil, err
	}

	if config.ConnTimeout <= 0 {
		config.ConnTimeout = DEFAULT_CONNECTION_TIMEOUT
	}

	if config.ReadTimeout <= 0 {
		config.ReadTimeout = DEFAULT_RW_TIMEOUT
	}

	if config.Uri == "" {
		config.Uri = "/"
	}

	if config.Port == 0 {
		return nil, fmt.Errorf("Missing option port")
	}

	return &HttpFetcher{
		httpFetcherConfig: config,
	}, nil
}

func (t *HttpFetcher) Fetch(task *common.FetcherTask) ([]byte, error) {
	logger.Infof("%s HTTPFetcher config: %v", task.Id, t.httpFetcherConfig)
	url := fmt.Sprintf("http://%s:%d%s",
		task.Target,
		t.Port,
		t.Uri)

	var (
		resp *http.Response
		err  error
	)
	if t.ConnTimeout == DEFAULT_CONNECTION_TIMEOUT && t.ReadTimeout == DEFAULT_RW_TIMEOUT {
		logger.Infof("%s requested URL: %s, default timeouts conn %v rw %v",
			task.Id, url, CONNECTION_TIMEOUT, RW_TIMEOUT)
		resp, err = HttpClient.Get(url)
		if err != nil {
			return nil, err
		}
	} else {
		connTimeout := time.Duration(t.ConnTimeout) * time.Millisecond
		rwTimeout := time.Duration(t.ReadTimeout) * time.Millisecond
		logger.Infof("%s requested URL: %s, nondefault timeouts: conn %v rw %v",
			task.Id, url, connTimeout, rwTimeout)
		httpCli := httpclient.NewClientWithTimeout(
			connTimeout, rwTimeout)
		httpCli.Transport.(*http.Transport).DisableKeepAlives = true
		resp, err = httpCli.Get(url)
		if err != nil {
			return nil, err
		}
	}
	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}
