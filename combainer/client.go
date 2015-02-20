package combainer

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/configs"
	"github.com/noxiouz/Combaine/common/hosts"
	"github.com/noxiouz/Combaine/common/tasks"
)

var (
	ErrAppUnavailable = fmt.Errorf("Application is unavailable")
	ErrHandlerTimeout = fmt.Errorf("Timeout")
)

type sessionParams struct {
	ParsingTime time.Duration
	WholeTime   time.Duration
	PTasks      []tasks.ParsingTask
	AggTasks    []tasks.AggregationTask
}

type Client struct {
	Id         string
	Repository configs.Repository
	*Context
	clientStats
}

func NewClient(context *Context, repo configs.Repository) (*Client, error) {
	if context.Hosts == nil {
		err := fmt.Errorf("Hosts delegate must be specified")
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Unable to create Client")
		return nil, err
	}

	cl := &Client{
		Id:         GenerateSessionId(),
		Repository: repo,
		Context:    context,
	}
	return cl, nil
}

func (cl *Client) UpdateSessionParams(config string) (sp *sessionParams, err error) {
	log.WithFields(log.Fields{
		"client": cl.Id,
		"config": config,
	}).Info("Updating session parametrs")

	var (
		// tasks
		pTasks   []tasks.ParsingTask
		aggTasks []tasks.AggregationTask

		// timeouts
		parsingTime time.Duration
		wholeTime   time.Duration
	)

	encodedParsingConfig, err := cl.Repository.GetParsingConfig(config)
	if err != nil {
		log.WithFields(log.Fields{
			"client": cl.Id,
			"config": config,
			"error":  err,
		}).Error("Unable to load config")
		return nil, err
	}

	var parsingConfig configs.ParsingConfig
	if err := encodedParsingConfig.Decode(&parsingConfig); err != nil {
		log.WithFields(log.Fields{
			"client": cl.Id,
			"config": config,
			"error":  err,
		}).Error("Unable to decode parsingConfig")
		return nil, err
	}

	cfg := cl.Repository.GetCombainerConfig()
	parsingConfig.UpdateByCombainerConfig(&cfg)
	aggregationConfigs, err := GetAggregationConfigs(cl.Repository, &parsingConfig)
	if err != nil {
		log.WithFields(log.Fields{
			"client": cl.Id,
			"config": config,
			"error":  err,
		}).Error("Unable to read aggregation configs")
		return nil, err
	}

	log.Infof("Updating config: group %s, metahost %s",
		parsingConfig.GetGroup(), parsingConfig.GetMetahost())

	hostFetcher, err := LoadHostFetcher(cl.Context, parsingConfig.HostFetcher)
	if err != nil {
		log.WithFields(log.Fields{
			"client": cl.Id,
			"config": config,
			"error":  err,
		}).Error("Unable to construct SimpleFetcher")
		return
	}

	allHosts := make(hosts.Hosts)
	for _, item := range parsingConfig.Groups {
		hosts_for_group, err := hostFetcher.Fetch(item)
		if err != nil {
			log.WithFields(log.Fields{
				"client": cl.Id,
				"config": config,
				"error":  err,
				"group":  item,
			}).Warn("Unable to get hosts")
			continue
		}

		allHosts.Merge(&hosts_for_group)
	}

	listOfHosts := allHosts.AllHosts()

	log.WithFields(log.Fields{
		"client": cl.Id,
		"config": config,
	}).Infof("Hosts: %s", listOfHosts)

	if len(listOfHosts) == 0 {
		err := fmt.Errorf("No hosts in given groups")
		log.WithFields(log.Fields{
			"client": cl.Id,
			"config": config,
			"group":  parsingConfig.Groups,
		}).Warn("No hosts in given groups")
		return nil, err
	}

	// Tasks for parsing
	for _, host := range listOfHosts {
		pTasks = append(pTasks, tasks.ParsingTask{
			CommonTask:         tasks.EmptyCommonTask,
			Host:               host,
			ParsingConfigName:  config,
			ParsingConfig:      parsingConfig,
			AggregationConfigs: *aggregationConfigs,
		})
	}

	for _, name := range parsingConfig.AggConfigs {
		aggTasks = append(aggTasks, tasks.AggregationTask{
			CommonTask:        tasks.EmptyCommonTask,
			Config:            name,
			ParsingConfigName: config,
			ParsingConfig:     parsingConfig,
			AggregationConfig: (*aggregationConfigs)[name],
			Hosts:             allHosts,
		})
	}

	parsingTime, wholeTime = GenerateSessionTimeFrame(parsingConfig.IterationDuration)

	sp = &sessionParams{
		ParsingTime: parsingTime,
		WholeTime:   wholeTime,
		PTasks:      pTasks,
		AggTasks:    aggTasks,
	}

	log.WithFields(log.Fields{
		"client": cl.Id,
		"config": config,
	}).Infof("Session parametrs have been updated successfully. %v", sp)
	return sp, nil
}

func (cl *Client) Dispatch(parsingConfigName string, uniqueID string, shouldWait bool) error {
	_observer.RegisterClient(cl, parsingConfigName)
	defer _observer.UnregisterClient(parsingConfigName)

	if uniqueID == "" {
		uniqueID = GenerateSessionId()
	}

	contextFields := log.Fields{
		"client":  cl.Id,
		"session": uniqueID,
		"config":  parsingConfigName}

	var deadline, startTime time.Time
	var wg sync.WaitGroup

	sessionParameters, err := cl.UpdateSessionParams(parsingConfigName)
	if err != nil {
		log.WithFields(log.Fields{
			"client":  cl.Id,
			"session": uniqueID,
			"config":  parsingConfigName,
			"error":   err,
		}).Error("Unable to update session parametrs")
		return err
	}

	startTime = time.Now()
	deadline = startTime.Add(sessionParameters.ParsingTime)

	log.WithFields(contextFields).Info("Start new iteration")

	hosts, err := cl.Context.Hosts()
	if err != nil || len(hosts) == 0 {
		log.WithFields(log.Fields{
			"client":  cl.Id,
			"session": uniqueID,
			"config":  parsingConfigName,
			"error":   err,
		}).Error("Unable to get (or empty) the list of the cloud hosts")

		return err
	}

	// Parsing phase
	totalTasksAmount := len(sessionParameters.PTasks)
	for i, task := range sessionParameters.PTasks {
		// Description of task
		task.PrevTime = startTime.Unix()
		task.CurrTime = startTime.Add(sessionParameters.WholeTime).Unix()
		task.CommonTask.Id = uniqueID

		log.WithFields(contextFields).Info("Send task number %d/%d to parsing %v", i+1, totalTasksAmount, task)

		wg.Add(1)
		go cl.doParsingTask(&task, &wg, deadline, hosts)
	}
	wg.Wait()

	log.WithFields(contextFields).Info("Parsing finished")

	// Aggregation phase
	deadline = startTime.Add(sessionParameters.WholeTime)
	totalTasksAmount = len(sessionParameters.AggTasks)
	for i, task := range sessionParameters.AggTasks {
		task.PrevTime = startTime.Unix()
		task.CurrTime = startTime.Add(sessionParameters.WholeTime).Unix()
		task.CommonTask.Id = uniqueID

		log.WithFields(contextFields).Info("Send task number %d/%d to aggregate %v", i+1, totalTasksAmount, task)

		wg.Add(1)
		go cl.doAggregationHandler(&task, &wg, deadline, hosts)
	}
	wg.Wait()

	log.WithFields(contextFields).Info("Aggregation has finished")

	// Wait for next iteration
	if shouldWait {
		time.Sleep(deadline.Sub(time.Now()))
	}

	log.WithFields(contextFields).Debug("Go to the next iteration")

	return nil
}

type resolveInfo struct {
	App *cocaine.Service
	Err error
}

func resolve(appname, endpoint string) <-chan resolveInfo {
	res := make(chan resolveInfo)
	go func() {
		app, err := cocaine.NewService(appname, endpoint)
		select {
		case res <- resolveInfo{
			App: app,
			Err: err,
		}:
		default:
			if err == nil {
				app.Close()
			}
		}
	}()
	return res
}

func (cl *Client) doGeneralTask(appName string, task tasks.Task, wg *sync.WaitGroup, deadline time.Time, hosts []string) error {
	defer (*wg).Done()
	limit := deadline.Sub(time.Now())

	var (
		err  error
		app  *cocaine.Service
		host string
	)

	for deadline.After(time.Now()) {
		host = fmt.Sprintf("%s:10053", getRandomHost(hosts))
		select {
		case r := <-resolve(appName, host):
			err, app = r.Err, r.App
		case <-time.After(1 * time.Second):
			err = fmt.Errorf("service resolvation was timeouted %s %s %s", task.Tid(), host, appName)
		}
		if err == nil {
			defer app.Close()
			log.WithFields(log.Fields{
				"client":  cl.Id,
				"session": task.Tid(),
				"host":    host,
				"appname": appName,
			}).Debug("application successfully connected")
			break
		}

		log.WithFields(log.Fields{
			"client":  cl.Id,
			"session": task.Tid(),
			"error":   err,
			"appname": appName,
			"host":    host,
		}).Warning("unable to connect to application")
		time.Sleep(200 * time.Microsecond)
	}

	if app == nil {
		log.WithFields(log.Fields{
			"client":  cl.Id,
			"session": task.Tid(),
			"error":   ErrAppUnavailable,
			"appname": appName,
		}).Error("unable to send task")
		return ErrAppUnavailable
	}

	raw, _ := task.Raw()
	res, err := PerformTask(app, raw, limit)
	if err != nil {
		log.WithFields(log.Fields{
			"client":  cl.Id,
			"session": task.Tid(),
			"error":   err,
			"appname": appName,
			"host":    host,
		}).Errorf("task for group %s failed", task.Group())
		return err
	}

	log.WithFields(log.Fields{
		"client":  cl.Id,
		"session": task.Tid(),
		"error":   err,
		"appname": appName,
		"host":    host,
	}).Infof("task for group %s done: %s", task.Group(), res)
	return nil
}

func (cl *Client) doParsingTask(task tasks.Task, wg *sync.WaitGroup, deadline time.Time, hosts []string) {
	if err := cl.doGeneralTask(common.PARSING, task, wg, deadline, hosts); err != nil {
		cl.clientStats.AddFailedParsing()
		return
	}
	cl.clientStats.AddSuccessParsing()
}

func (cl *Client) doAggregationHandler(task tasks.Task, wg *sync.WaitGroup, deadline time.Time, hosts []string) {
	if err := cl.doGeneralTask(common.AGGREGATE, task, wg, deadline, hosts); err != nil {
		cl.clientStats.AddFailedAggregate()
		return
	}
	cl.clientStats.AddSuccessAggregate()
}

func getRandomHost(input []string) string {
	max := len(input)
	return input[rand.Intn(max)]
}

func PerformTask(app *cocaine.Service, payload []byte, limit time.Duration) (interface{}, error) {
	select {
	case res := <-app.Call("enqueue", "handleTask", payload):
		if res.Err() != nil {
			return nil, res.Err()
		}
		var i interface{}
		err := res.Extract(&i)
		return i, err
	case <-time.After(limit):
	}
	return nil, ErrHandlerTimeout
}
