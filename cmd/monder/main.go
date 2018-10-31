package main

import (
	"log"
	"strconv"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/senders/juggler"
	"github.com/combaine/combaine/utils"
	"github.com/globalsign/mgo"
)

type senderTask struct {
	ID     string `codec:"Id"`
	Data   []common.AggregationResult
	Config juggler.Config
}

var (
	cfg     *juggler.SenderConfig
	session *mgo.Session
)

func send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()
	var task senderTask
	err := utils.Unpack(raw, &task)
	if err != nil {
		logger.Errf("%s Failed to unpack task %s", task.ID, err)
		return
	}
	logger.Infof("%s Task len: %v documents save to '%s.%s'", task.ID, len(task.Data), cfg.Store.Database, task.Config.Namespace)
	c := session.DB(cfg.Store.Database).C(task.Config.Namespace)
	for _, d := range task.Data {
		d.Tags["ts"] = strconv.FormatInt(time.Now().Unix(), 64)
		if err = c.Insert(d); err != nil {
			// https://godoc.org/gopkg.in/mgo.v2#Session.Refresh
			session.Refresh()
			logger.Errf("%s Failed to insert: %s", task.ID, err)
		}
	}

	response.Write("DONE")
}

func main() {
	var err error
	cfg, err = juggler.GetSenderConfig()
	if err != nil {
		log.Fatalf("Failed to load sender config %s", err)
	}
	logger.MustCreateLogger()

	logger.Infof("Connect to %s with timeout %ds", cfg.Store.Cluster, cfg.Store.Timeout)
	session, err = mgo.DialWithTimeout(cfg.Store.Cluster, time.Duration(cfg.Store.Timeout)*time.Second)
	if err != nil {
		log.Fatalf("Failed to connect mongo %s", err)
	}
	session.SetMode(mgo.Eventual, true)

	if cfg.Store.User != "" {
		err = session.DB(cfg.Store.AuthDB).Login(cfg.Store.User, cfg.Store.Password)
		if err != nil {
			log.Fatalf("Failed to login mongo %s", err)
		}
	}
	binds := map[string]cocaine.EventHandler{
		"send": send,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
