package aggregating

import (
	"fmt"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"
)

func enqueue(app *cocaine.Service, payload []byte) []byte {

	select {
	case res := <-app.Call("enqueue", "aggregate_group", payload):
		if res == nil {
			return nil, fmt.Errorf("App aggregate_group unavailable")
		}
		if res.Err() != nil {
			return nil, fmt.Errorf("Task failed  %s", res.Err())
		}

		var raw_res []byte
		if err := res.Extract(&raw_res); err != nil {
			return nil, fmt.Errorf("Unable to extract result. %s", err.Error())
		}

	case <-time.After(deadline):
		return nil, fmt.Errorf("Timeout %s", deadline)
	}
	return raw_res, nil
}
