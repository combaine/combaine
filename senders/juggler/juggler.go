package juggler

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/logger"
	lua "github.com/yuin/gopher-lua"
)

// Sender main object
type Sender struct {
	*Config
	id    string
	state *lua.LState
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
	checks, err := js.getCheck(ctx)
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
	var jWg sync.WaitGroup
	var sendEeventsFailed int32
	for _, jFront := range js.Config.JFrontend {
		jWg.Add(1)
		go func(front string, jEs []jugglerEvent) {
			defer jWg.Done()
			for _, e := range jEs {
				if err := js.sendEvent(ctx, front, e); err != nil {
					atomic.AddInt32(&sendEeventsFailed, 1)
					logger.Errf("%s failed to send juggler Event %s: %s", js.id, e, err)
				}
			}
		}(jFront, events)
	}
	jWg.Wait()

	if sendEeventsFailed > 0 {
		msg := fmt.Errorf("failed to send %d/%d events", sendEeventsFailed, len(events)*len(js.Config.JFrontend))
		logger.Errf("%s %s", js.id, msg.Error())
		return msg
	}
	logger.Infof("%s successfully send %d events", js.id, len(events))

	return nil
}
