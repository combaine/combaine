package main

import (
	"bufio"
	"bytes"
	"context"
	"log"
	"os"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/senders/agave"
)

var (
	logger        *cocaine.Logger
	defaultFields = []string{
		"75_prc", "90_prc", "93_prc",
		"94_prc", "95_prc", "96_prc",
		"97_prc", "98_prc", "99_prc",
	}
	defaultStep    int64 = 300
	defaultTimeout       = 5 * time.Second
)

type agaveTask struct {
	ID     string `codec:"Id"`
	Data   []common.AggregationResult
	Config agave.Config
}

func getAgaveHosts() ([]string, error) {
	var path = os.Getenv("config")
	if len(path) == 0 {
		path = "/etc/combaine/agave.conf"
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var result []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		result = append(result, string(bytes.TrimSpace(scanner.Bytes())))
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// Send parse cocaine Requset and send all metrics to agave hosts
func Send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()
	var task agaveTask
	err := common.Unpack(raw, &task)
	if err != nil {
		logger.Errf("%s Failed to unpack agave task %s", task.ID, err)
		response.ErrorMsg(-100, err.Error())
		return
	}
	logger.Debugf("%s Task: %v", task.ID, task)

	task.Config.Hosts, err = getAgaveHosts()
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}

	if len(task.Config.Fields) == 0 {
		task.Config.Fields = defaultFields
	}

	if task.Config.Step == 0 {
		task.Config.Step = defaultStep
	}

	logger.Debugf("%s Fields: %v Step: %d", task.ID, task.Config.Fields, task.Config.Step)

	as, err := agave.NewSender(task.ID, task.Config)
	if err != nil {
		logger.Errf("%s Unexpected error %s", task.ID, err)
		response.ErrorMsg(-100, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	as.Send(ctx, task.Data)
	response.Write("OK")
}

func main() {
	var err error
	logger, err = cocaine.NewLogger()
	binds := map[string]cocaine.EventHandler{
		"send": Send,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
