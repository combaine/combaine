package juggler

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/common/tasks"
	lua "github.com/yuin/gopher-lua"
)

type jugglerSender struct {
	*JugglerConfig
	id    string
	state *lua.LState
}

func NewJugglerSender(conf *JugglerConfig, id string) (*jugglerSender, error) {
	return &jugglerSender{
		JugglerConfig: conf,
		id:            id,
		state:         nil,
	}, nil
}

// Send make all things abount juggler sender tasks
func (js *jugglerSender) Send(ctx context.Context, data []tasks.AggregationResult) error {
	logger.Debugf("%s Load lua plugin %s", js.id, js.Plugin)
	state, err := LoadPlugin(js.PluginsDir, js.Plugin)
	if err != nil {
		return err
	}
	js.state = state

	logger.Debugf("%s Prepare state of lua plugin", js.id)
	if err := js.preparePluginEnv(data); err != nil {
		return err
	}

	jEvents, err := js.runPlugin()
	if err != nil {
		return err
	}
	checks, err := js.getCheck(ctx)
	if err != nil {
		return err
	}
	if err := js.ensureCheck(ctx, checks, jEvents); err != nil {
		return err
	}

	if err := js.sendInternal(ctx, jEvents); err != nil {
		return err
	}
	return nil
}

func (js *jugglerSender) sendInternal(ctx context.Context, events []jugglerEvent) error {
	var jWg sync.WaitGroup
	var sendEeventsFailed int32
	for _, jFront := range js.JugglerConfig.JFrontend {
		jWg.Add(1)
		go func(front string, jEs []jugglerEvent, wg *sync.WaitGroup) {
			defer wg.Done()
			for _, e := range jEs {
				if err := js.sendEvent(ctx, front, e); err != nil {
					atomic.AddInt32(&sendEeventsFailed, 1)
					logger.Errf("%s failed to send juggler Event %s: %s", js.id, e, err)
				}
			}
		}(jFront, events, &jWg)
	}
	jWg.Wait()

	if sendEeventsFailed > 0 {
		msg := fmt.Errorf("failed to send %d/%d events", sendEeventsFailed, len(events))
		logger.Errf("%s %s", js.id, msg.Error())
		return msg
	}
	logger.Infof("%s successfully send %d events", js.id, len(events))

	return nil
}
