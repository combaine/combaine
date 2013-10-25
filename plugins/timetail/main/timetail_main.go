package main

import (
	"fmt"
	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/noxiouz/Combaine/plugins/timetail"
	//"github.com/ugorji/go/codec"
	"encoding/json"
	"log"
)

var (
	logger *cocaine.Logger
)

func UnpackRequest(raw_data []byte) (*timetail.InputData, error) {
	var res timetail.InputData
	if err := json.Unmarshal(raw_data, &res); err != nil {
		log.Println(err)
		return nil, err
	}
	return &res, nil
}

func get(request *cocaine.Request, response *cocaine.Response) {
	incoming := <-request.Read()
	task, err := UnpackRequest(incoming)
	if err != nil {
		response.ErrorMsg(1, fmt.Sprintf("%v", err))
		response.Close()
		return
	}

	logger.Info(fmt.Sprintf("%v", task))

	res, err := timetail.Get(task.AsString())
	if err != nil {
		response.ErrorMsg(1, fmt.Sprintf("%v", err))
		response.Close()
		return
	}

	response.Write(res)
	response.Close()
}

func main() {
	logger = cocaine.NewLogger()
	binds := map[string]cocaine.EventHandler{
		"get": get,
	}
	Worker := cocaine.NewWorker()
	Worker.Loop(binds)
}
