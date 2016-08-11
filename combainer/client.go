package combainer

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
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
	ParallelParsings int
	ParsingTime      time.Duration
	WholeTime        time.Duration
	PTasks           []tasks.ParsingTask
	AggTasks         []tasks.AggregationTask
}

type Client struct {
	Id         string
	Repository configs.Repository
	*Context
	Log *logrus.Entry
	clientStats
}

func NewClient(context *Context, repo configs.Repository) (*Client, error) {
	if context.Hosts == nil {
		err := fmt.Errorf("Hosts delegate must be specified")
		context.Logger.WithFields(logrus.Fields{
			"error": err,
		}).Error("Unable to create Client")
		return nil, err
	}

	id := GenerateSessionId()
	cl := &Client{
		Id:         id,
		Repository: repo,
		Context:    context,
		Log:        context.Logger.WithField("client", id),
	}
	return cl, nil
}

func (cl *Client) UpdateSessionParams(config string) (sp *sessionParams, err error) {
	cl.Log.WithFields(logrus.Fields{
		"config": config,
	}).Info("updating session parametrs")

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
		cl.Log.WithFields(logrus.Fields{
			"config": config,
			"error":  err,
		}).Error("unable to load config")
		return nil, err
	}

	var parsingConfig configs.ParsingConfig
	if err := encodedParsingConfig.Decode(&parsingConfig); err != nil {
		cl.Log.WithFields(logrus.Fields{
			"config": config,
			"error":  err,
		}).Error("unable to decode parsingConfig")
		return nil, err
	}

	cfg := cl.Repository.GetCombainerConfig()
	parsingConfig.UpdateByCombainerConfig(&cfg)
	aggregationConfigs, err := GetAggregationConfigs(cl.Repository, &parsingConfig)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{
			"config": config,
			"error":  err,
		}).Error("unable to read aggregation configs")
		return nil, err
	}

	cl.Log.Infof("updating config: group %s, metahost %s",
		parsingConfig.GetGroup(), parsingConfig.GetMetahost())

	hostFetcher, err := LoadHostFetcher(cl.Context, parsingConfig.HostFetcher)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{
			"config": config,
			"error":  err,
		}).Error("Unable to construct SimpleFetcher")
		return
	}

	allHosts := make(hosts.Hosts)
	for _, item := range parsingConfig.Groups {
		hosts_for_group, err := hostFetcher.Fetch(item)
		if err != nil {
			cl.Log.WithFields(logrus.Fields{
				"config": config,
				"error":  err,
				"group":  item,
			}).Warn("unable to get hosts")
			continue
		}

		allHosts.Merge(&hosts_for_group)
	}

	listOfHosts := allHosts.AllHosts()

	if len(listOfHosts) == 0 {
		err := fmt.Errorf("No hosts in given groups")
		cl.Log.WithFields(logrus.Fields{
			"config": config,
			"group":  parsingConfig.Groups,
		}).Warn("no hosts in given groups")
		return nil, err
	}

	cl.Log.WithFields(logrus.Fields{
		"config": config,
	}).Infof("hosts: %s", listOfHosts)

	parallelParsings := len(listOfHosts)
	if parsingConfig.ParallelParsings > 0 && parallelParsings > parsingConfig.ParallelParsings {
		parallelParsings = parsingConfig.ParallelParsings
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
		ParallelParsings: parallelParsings,
		ParsingTime:      parsingTime,
		WholeTime:        wholeTime,
		PTasks:           pTasks,
		AggTasks:         aggTasks,
	}

	cl.Log.Info("Session parametrs have been updated successfully")
	cl.Log.WithFields(logrus.Fields{
		"config": config,
	}).Debugf("Current session parametrs. %v", sp)

	return sp, nil
}

func (cl *Client) Dispatch(parsingConfigName string, uniqueID string, shouldWait bool) error {
	GlobalObserver.RegisterClient(cl, parsingConfigName)
	defer GlobalObserver.UnregisterClient(parsingConfigName)

	if uniqueID == "" {
		uniqueID = GenerateSessionId()
	}

	contextFields := logrus.Fields{
		"session": uniqueID,
		"config":  parsingConfigName}

	var deadline, startTime time.Time
	var wg sync.WaitGroup

	sessionParameters, err := cl.UpdateSessionParams(parsingConfigName)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{
			"session": uniqueID,
			"config":  parsingConfigName,
			"error":   err,
		}).Error("unable to update session parametrs")
		return err
	}

	startTime = time.Now()
	deadline = startTime.Add(sessionParameters.ParsingTime)

	cl.Log.WithFields(contextFields).Info("Start new iteration")

	hosts, err := cl.Context.Hosts()
	if err != nil || len(hosts) == 0 {
		cl.Log.WithFields(logrus.Fields{
			"session": uniqueID,
			"config":  parsingConfigName,
			"error":   err,
		}).Error("unable to get (or empty) the list of the cloud hosts")

		return err
	}

	// Parsing phase
	totalTasksAmount := len(sessionParameters.PTasks)
	tokens := make(chan struct{}, sessionParameters.ParallelParsings)
	parsingResult := make(tasks.Result)
	var mu sync.Mutex

	for i, task := range sessionParameters.PTasks {
		// Description of task
		task.PrevTime = startTime.Unix()
		task.CurrTime = startTime.Add(sessionParameters.WholeTime).Unix()
		task.CommonTask.Id = uniqueID

		cl.Log.WithFields(contextFields).Infof(
			"Send task number %d/%d to parsing %v", i+1, totalTasksAmount, task)

		wg.Add(1)
		tokens <- struct{}{} // acqure
		go cl.doParsingTask(task, &wg, &mu, tokens, deadline, hosts, parsingResult)
	}
	wg.Wait()

	cl.Log.WithFields(contextFields).Infof(
		"Parsing finished for %d hosts", len(parsingResult))

	// Aggregation phase
	deadline = startTime.Add(sessionParameters.WholeTime)
	totalTasksAmount = len(sessionParameters.AggTasks)
	for i, task := range sessionParameters.AggTasks {
		task.PrevTime = startTime.Unix()
		task.CurrTime = startTime.Add(sessionParameters.WholeTime).Unix()
		task.CommonTask.Id = uniqueID

		cl.Log.WithFields(contextFields).Infof(
			"Send task number %d/%d to aggregate %v", i+1, totalTasksAmount, task)

		wg.Add(1)
		go cl.doAggregationHandler(task, &wg, deadline, hosts, parsingResult)
	}
	wg.Wait()

	cl.Log.WithFields(contextFields).Info("Aggregation has finished")

	// Wait for next iteration
	if shouldWait {
		time.Sleep(deadline.Sub(time.Now()))
	}

	cl.Log.WithFields(contextFields).Debug("Go to the next iteration")

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

func (cl *Client) doGeneralTask(appName string, task tasks.Task,
	wg *sync.WaitGroup, deadline time.Time, hosts []string) (interface{}, error) {

	defer (*wg).Done()
	limit := deadline.Sub(time.Now())

	var (
		err  error
		app  *cocaine.Service
		host string
	)

	for deadline.After(time.Now()) {
		host = fmt.Sprintf("%s:10053", getRandomHost(appName, hosts))
		select {
		case r := <-resolve(appName, host):
			err, app = r.Err, r.App
		case <-time.After(1 * time.Second):
			err = fmt.Errorf("service resolvation was timeouted %s %s %s",
				task.Tid(), host, appName)
		}
		if err == nil {
			defer app.Close()
			cl.Log.WithFields(logrus.Fields{
				"session": task.Tid(),
				"host":    host,
				"appname": appName,
			}).Debug("application successfully connected")
			break
		}

		cl.Log.WithFields(logrus.Fields{
			"session": task.Tid(),
			"error":   err,
			"appname": appName,
			"host":    host,
		}).Warning("unable to connect to application")
		time.Sleep(200 * time.Microsecond)
	}

	if app == nil {
		cl.Log.WithFields(logrus.Fields{
			"session": task.Tid(),
			"error":   ErrAppUnavailable,
			"appname": appName,
		}).Error("unable to send task")
		return nil, ErrAppUnavailable
	}

	raw, err := task.Raw()
	if err != nil {
		cl.Log.WithFields(logrus.Fields{
			"session": task.Tid(),
			"error":   err,
			"appname": appName,
			"host":    host,
		}).Errorf("failed to unpack task's data for group %s", task.Group())
		return nil, err

	}
	res, err := PerformTask(app, raw, limit)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{
			"session": task.Tid(),
			"error":   err,
			"appname": appName,
			"host":    host,
		}).Errorf("task for group %s failed", task.Group())
		return nil, err
	}

	cl.Log.WithFields(logrus.Fields{
		"session": task.Tid(),
		"appname": appName,
		"host":    host,
	}).Infof("task for group %s done", task.Group())
	return res, nil
}

func (cl *Client) doParsingTask(task tasks.ParsingTask,
	wg *sync.WaitGroup, m *sync.Mutex, tokens <-chan struct{},
	deadline time.Time, hosts []string, r tasks.Result) {
	defer func() { <-tokens }() // release

	i, err := cl.doGeneralTask(common.PARSING, &task, wg, deadline, hosts)
	if err != nil {
		cl.clientStats.AddFailedParsing()
		return
	}
	var res tasks.Result
	if err := common.Unpack(i.([]byte), &res); err != nil {
		cl.clientStats.AddFailedParsing()
		return
	}
	m.Lock()
	for k, v := range res {
		r[k] = v
	}
	m.Unlock()
	cl.clientStats.AddSuccessParsing()

}

func (cl *Client) doAggregationHandler(task tasks.AggregationTask,
	wg *sync.WaitGroup, deadline time.Time, hosts []string, r tasks.Result) {

	task.ParsingResult = r
	_, err := cl.doGeneralTask(common.AGGREGATE, &task, wg, deadline, hosts)
	if err != nil {
		cl.clientStats.AddFailedAggregate()
		return
	}
	cl.clientStats.AddSuccessAggregate()
}

func getRandomHost(app string, input []string) string {
	if app == common.AGGREGATE {
		return "localhost"
	}
	max := len(input)
	return input[rand.Intn(max)]
}

func PerformTask(app *cocaine.Service,
	payload []byte, limit time.Duration) (interface{}, error) {

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
