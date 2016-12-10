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

	logger.Debugf("%s Prepare plugin state", js.id)
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
	fmt.Printf("jEvents: %v", jEvents)
	if err := js.ensureCheck(ctx, checks, jEvents); err != nil {
		return err
	}

	var jWg sync.WaitGroup
	var sendEeventsFailed int32
	// TODO send evnets to all juggler fronts
	// use builk request?
	for _, e := range jEvents {
		jWg.Add(1)
		go func(jEv jugglerEvent, wg *sync.WaitGroup) {
			defer wg.Done()
			if err := js.sendEvent(jEv); err != nil {
				atomic.AddInt32(&sendEeventsFailed, 1)
				logger.Errf("%s failed to send juggler Event: %s", js.id, err)
			}
		}(e, &jWg)
	}
	jWg.Wait()

	if sendEeventsFailed > 0 {
		msg := fmt.Errorf("failed to send %d/%d events", sendEeventsFailed, len(jEvents))
		logger.Errf("%s %s", js.id, msg.Error())
		return msg
	}
	logger.Infof("%s successfully send %d events", js.id, len(jEvents))

	return nil
}
