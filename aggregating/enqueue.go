package aggregating

import (
	"fmt"

	"github.com/cocaine/cocaine-framework-go/cocaine"
)

func enqueue(app *cocaine.Service, payload []byte) ([]byte, error) {
	var raw_res []byte

	res := <-app.Call("enqueue", "aggregate_group", payload)
	if res == nil {
		return nil, fmt.Errorf("App aggregate_group unavailable")
	}
	if res.Err() != nil {
		return nil, fmt.Errorf("Task failed  %s", res.Err())
	}

	if err := res.Extract(&raw_res); err != nil {
		return nil, fmt.Errorf("Unable to extract result. %s", err.Error())
	}
	return raw_res, nil
}
