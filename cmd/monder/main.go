package main

import (
	"log"
	"strconv"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/senders/juggler"
	"github.com/combaine/combaine/utils"
	"github.com/globalsign/mgo"
)

var (
	cfg     *juggler.SenderConfig
	session *mgo.Session
)
var ch = make(chan *juggler.SenderTask, 10) // 10 tasks

func storeWorker() {
	for task := range ch {
		c := session.DB(cfg.Store.Database).C(task.Config.Namespace)
		for _, d := range task.Data {
			d.Tags["ts"] = strconv.FormatInt(task.CurrTime, 10)
			if err := c.Insert(d); err != nil {
				// https://godoc.org/gopkg.in/mgo.v2#Session.Refresh
				session.Refresh()
				logger.Errf("%s Failed to insert: %s", task.Id, err)
			}
			logger.Infof("%s task saved to '%s.%s'", task.Id, cfg.Store.Database, task.Config.Namespace)
		}
	}
}

func send(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()

	raw := <-request.Read()
	var task juggler.SenderTask
	if err := utils.Unpack(raw, &task); err != nil {
		logger.Errf("%s Failed to unpack task %s", task.Id, err)
		return
	}
	deadline := time.Unix(task.CurrTime, 0)
	select {
	case ch <- &task:
		logger.Infof("%s Task %v bytes send to background worker", task.Id, len(raw))
	case <-time.After(time.Until(deadline)):
		logger.Errf("%s Timeout while send to store")
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

	timeout := 30 * time.Second
	logger.Infof("Connect to %s with timeout %v", cfg.Store.Cluster, timeout)
	session, err = mgo.DialWithTimeout(cfg.Store.Cluster, timeout)
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
	go storeWorker()
	Worker.Loop(binds)
}
