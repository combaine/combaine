package aggregating

import (
	"fmt"

	"github.com/Combaine/Combaine/common/servicecacher"
)

func enqueue(method string, app servicecacher.Service, payload []byte) ([]byte, error) {
	var raw_res []byte

	res := <-app.Call("enqueue", method, payload)
	if res == nil {
		return nil, fmt.Errorf("Failed to call application method '%s'", method)
	}
	if res.Err() != nil {
		return nil, fmt.Errorf("Task failed  %s", res.Err())
	}

	if err := res.Extract(&raw_res); err != nil {
		return nil, fmt.Errorf("Unable to extract result. %s", err.Error())
	}
	return raw_res, nil
}
