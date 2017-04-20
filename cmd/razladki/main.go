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
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/senders/razladki"
)

const (
	defaultConfigPath = "/etc/combaine/razladki.conf"
	defaultTimeout    = 5 * time.Second
)

var defaultFields = []string{
	"75_prc",
	"90_prc",
	"93_prc",
	"94_prc",
	"95_prc",
	"96_prc",
	"97_prc",
	"98_prc",
	"99_prc",
}

type razladkiTask struct {
	ID       string `codec:"Id"`
	Data     []common.AggregationResult
	Config   razladki.Config
	CurrTime uint64
	PrevTime uint64
}

func getRazladkiHost() (string, error) {
	var path = os.Getenv("config")
	if len(path) == 0 {
		path = defaultConfigPath
	}

	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		return string(bytes.TrimSpace(scanner.Bytes())), nil
	}

	return "", scanner.Err()
}

// Send parse cocaine request and send points to razladki service
func Send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()
	var task razladkiTask
	err := common.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}

	if len(task.Config.Fields) == 0 {
		task.Config.Fields = defaultFields
	}

	task.Config.Host, err = getRazladkiHost()
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}

	logger.Debugf("%s Task: %v", task.ID, task)

	rCli, err := razladki.NewSender(&task.Config, task.ID)
	if err != nil {
		logger.Errf("%s Unexpected error %s", task.ID, err)
		response.ErrorMsg(-100, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	err = rCli.Send(ctx, task.Data, task.PrevTime)
	if err != nil {
		logger.Errf("%s Sending error %s", task.ID, err)
		response.ErrorMsg(-100, err.Error())
		return
	}
	response.Write("DONE")
}

func main() {
	razladki.InitializeLogger(logger.MustCreateLogger)

	binds := map[string]cocaine.EventHandler{
		"send": Send,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
