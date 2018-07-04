package juggler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/common/chttp"
	"github.com/combaine/combaine/common/logger"
	lua "github.com/yuin/gopher-lua"
)

// Sender main object
type Sender struct {
	*Config
	id    string
	state *lua.LState
}

// GlobalCache for juggler checks
var GlobalCache *cache.TTLCache

// InitializeLogger create cocaine logger
func InitializeLogger(init func() logger.Logger) {
	_ = init() // init logger
	GlobalCache = cache.NewCache(time.Minute /* ttl */, time.Minute*5, time.Minute*5)
}

// NewSender return sender object with specified config
func NewSender(conf *Config, id string) (*Sender, error) {
	return &Sender{
		Config: conf,
		id:     id,
		state:  nil,
	}, nil
}

// Send make all things abount juggler sender tasks
func (js *Sender) Send(ctx context.Context, data []common.AggregationResult) error {
	logger.Debugf("%s Load lua plugin %s", js.id, js.Plugin)
	state, err := LoadPlugin(js.id, js.PluginsDir, js.Plugin)
	if err != nil {
		return fmt.Errorf("LoadPlugin: %s", err)
	}
	defer state.Close() // see TODO in LoadPlugin
	js.state = state

	logger.Debugf("%s Prepare state of lua plugin", js.id)
	if err := js.preparePluginEnv(data); err != nil {
		return fmt.Errorf("preparePluginEnv: %s", err)
	}

	jEvents, err := js.runPlugin()
	if err != nil {
		return fmt.Errorf("runPlugin: %s", err)
	}
	if len(jEvents) == 0 {
		logger.Infof("%s Nothing to send", js.id)
		return nil
	}
	checks, err := js.getCheck(ctx, jEvents)
	if err != nil {
		return fmt.Errorf("getCheck: %s", err)
	}
	if err := js.ensureCheck(ctx, checks, jEvents); err != nil {
		return fmt.Errorf("ensureCheck: %s", err)
	}

	if err := js.sendInternal(ctx, jEvents); err != nil {
		return fmt.Errorf("sendInternal: %s", err)
	}
	return nil
}

func (js *Sender) sendInternal(ctx context.Context, events []jugglerEvent) error {
	var (
		jWg               sync.WaitGroup
		sendEeventsFailed int32
		total             = len(events) * len(js.Config.JFrontend)
	)
	if js.Config.BatchEndpoint == "" {
		for _, event := range events {
			for _, jFront := range js.Config.JFrontend {
				jWg.Add(1)
				go func(e jugglerEvent, f string) {
					defer jWg.Done()
					if err := js.sendEvent(ctx, f, e); err != nil {
						atomic.AddInt32(&sendEeventsFailed, 1)
						logger.Errf("%s failed to send juggler Event %s: %s", js.id, e, err)
					}
				}(event, jFront)
			}
		}
	} else {
		total = len(events)
		for len(events) > 0 {
			batch := events
			if len(events) > js.Config.BatchSize {
				batch = events[:js.Config.BatchSize]
				events = events[js.Config.BatchSize:]
			} else {
				events = []jugglerEvent{}
			}
			jWg.Add(1)
			go func(je []jugglerEvent, f string) {
				defer jWg.Done()
				logger.Infof("%s Send batch %d events to %s", js.id, len(je), f)

				b := jugglerBatchRequest{
					Events: je,
					Source: "combainer " + common.Hostname(),
				}
				batchJSON, err := json.Marshal(b)

				if err != nil {
					logger.Errf("%s failed to Marshal batch %s", js.id, err)
					atomic.AddInt32(&sendEeventsFailed, int32(len(je)))
					return
				}
				respJSON, err := js.sendBatch(ctx, batchJSON, f)
				logger.Debugf("Juggler response %s", respJSON)
				if err != nil {
					atomic.AddInt32(&sendEeventsFailed, int32(len(je)))
					logger.Errf("%s failed to send juggler batch with %d events: %s", js.id, len(je), err)
				}
				var resp jugglerBatchResponse
				err = json.Unmarshal(respJSON, &resp)
				if err != nil {
					logger.Errf("%s Failed to unmarhal response %s", js.id, err)
					return
				}
				if resp.Error != nil {
					logger.Errf("%s Failed to send batch: %v", js.id, resp.Error)
					atomic.AddInt32(&sendEeventsFailed, int32(len(je)))
				}
				for idx, e := range resp.Events {
					if e.Code != 200 {
						atomic.AddInt32(&sendEeventsFailed, 1)
						logger.Errf("%s Failed to send event %v: %s", js.id, je[idx], e.Message)
					}
				}
			}(batch, js.Config.BatchEndpoint)
		}
	}
	jWg.Wait()

	if sendEeventsFailed > 0 {
		return fmt.Errorf("failed to send %d/%d events", sendEeventsFailed, total)
	}
	logger.Infof("%s successfully send %d events", js.id, total)

	return nil
}

// sendBatch send batch events borned by ensureCheck into batch endpoint
func (js *Sender) sendBatch(ctx context.Context, batch []byte, endpoint string) ([]byte, error) {
	var (
		cancel       func()
		resp         *http.Response
		err          error
		responseBody []byte
		retry        = 0
	)

SEND_LOOP:
	for retry < 2 {
		retry++
		logger.Debugf("%s Attempt %d", js.id, retry)
		resp, err = chttp.Post(ctx, endpoint, "application/json", bytes.NewReader(batch))
		switch err {
		case nil:
			responseBody, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				logger.Errf("%s Failed to read response from %s: %s", js.id, endpoint, err)
			}
			resp.Body.Close()
			// err is nil and there may occure some http errors including timeout
			if resp.StatusCode == http.StatusOK {
				err = nil // override err and leave responseBody in undefined state
				logger.Infof("%s successfully sent data in %d attempts", js.id, retry)
				break SEND_LOOP
			}
			err = fmt.Errorf("http status='%s', response: %s", resp.Status, responseBody)
		case context.Canceled, context.DeadlineExceeded:
			ctx, cancel = context.WithTimeout(context.Background(), DefaultTimeout)
		default:
		}

		logger.Errf("%s failed to send: %s. Attempt %d", js.id, err, retry)
		if resp != nil && resp.StatusCode == 400 {
			break
		}
		if retry < 2 {
			time.Sleep(time.Millisecond * 100)
		}
	}
	if cancel != nil {
		cancel()
	}
	return responseBody, err
}

// sendEvent send juggler event borned by ensureCheck to jugglers
func (js *Sender) sendEvent(ctx context.Context, front string, event jugglerEvent) error {
	query := url.Values{
		"status":      {event.Status},
		"description": {event.Description},
		"service":     {event.Service},
		"host":        {event.Host},
		"instance":    {""},
	}

	url := fmt.Sprintf(sendEventURL, front, query.Encode())
	logger.Debugf("%s Send event %s", js.id, url)
	resp, err := chttp.Get(ctx, url)
	if err != nil {
		logger.Errf("%s %s", js.id, err)
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		logger.Errf("%s %s", js.id, err)
		return err
	}
	logger.Infof("%s Response %s: %d - %q", js.id, url, resp.StatusCode, body)

	if resp.StatusCode != http.StatusOK {
		return errors.New(string(body))
	}
	return nil
}
