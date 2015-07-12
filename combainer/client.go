package combainer

import (
	"fmt"
	"golang.org/x/net/context"
	"math/rand"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/noxiouz/Combaine/combainer/slave"
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
	Log *logrus.Entry
	clientStats
	context context.Context
}

func NewClient(ctx *Context, repo configs.Repository) (*Client, error) {
	if ctx.Hosts == nil {
		err := fmt.Errorf("Hosts delegate must be specified")
		ctx.Logger.WithFields(logrus.Fields{
			"error": err,
		}).Error("Unable to create Client")
		return nil, err
	}

	id := GenerateSessionId()
	cl := &Client{
		Id:         id,
		Repository: repo,
		Context:    ctx,
		Log:        ctx.Logger.WithField("client", id),
		context:    context.Background(),
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

	cl.Log.WithFields(logrus.Fields{
		"config": config,
	}).Infof("Session parametrs have been updated successfully. %v", sp)
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
	parsingCtx, parsingCancel := context.WithDeadline(cl.context, deadline)
	for i, task := range sessionParameters.PTasks {
		// Description of task
		task.PrevTime = startTime.Unix()
		task.CurrTime = startTime.Add(sessionParameters.WholeTime).Unix()
		task.CommonTask.Id = uniqueID

		cl.Log.WithFields(contextFields).Infof("Send task number %d/%d to parsing %v", i+1, totalTasksAmount, task)

		wg.Add(1)
		go func() {
			defer wg.Done()
			cl.doParsingTask(parsingCtx, task, hosts)
		}()
	}
	wg.Wait()
	// release all resources connected with context
	parsingCancel()

	cl.Log.WithFields(contextFields).Info("Parsing finished")

	// Aggregation phase
	deadline = startTime.Add(sessionParameters.WholeTime)
	totalTasksAmount = len(sessionParameters.AggTasks)
	aggContext, aggCancel := context.WithDeadline(cl.context, deadline)

	for i, task := range sessionParameters.AggTasks {
		task.PrevTime = startTime.Unix()
		task.CurrTime = startTime.Add(sessionParameters.WholeTime).Unix()
		task.CommonTask.Id = uniqueID

		cl.Log.WithFields(contextFields).Infof("Send task number %d/%d to aggregate %v", i+1, totalTasksAmount, task)

		wg.Add(1)
		go func() {
			defer wg.Done()
			cl.doAggregationHandler(aggContext, task, hosts)
		}()
	}
	wg.Wait()
	// release all resources connected with context
	aggCancel()

	cl.Log.WithFields(contextFields).Info("Aggregation has finished")

	// Wait for next iteration
	if shouldWait {
		time.Sleep(deadline.Sub(time.Now()))
	}

	cl.Log.WithFields(contextFields).Debug("Go to the next iteration")

	return nil
}

type resolveInfo struct {
	Slave slave.Slave
	Err   error
}

func resolve(appname, endpoint string) <-chan resolveInfo {
	res := make(chan resolveInfo)
	go func() {
		app, err := cocaine.NewService(appname, endpoint)
		select {
		case res <- resolveInfo{
			Slave: slave.NewSlave(app),
			Err:   err,
		}:
		default:
			if err == nil {
				app.Close()
			}
		}
	}()
	return res
}

func (cl *Client) doGeneralTask(ctx context.Context, appName string, task tasks.Task, hosts []string) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("no deadline is set")
	}

	var (
		err   error
		slave slave.Slave
		host  string
	)

	for deadline.After(time.Now()) {
		host = fmt.Sprintf("%s:10053", getRandomHost(hosts))
		select {
		case r := <-resolve(appName, host):
			err, slave = r.Err, r.Slave
		case <-time.After(1 * time.Second):
			err = fmt.Errorf("service resolvation was timeouted %s %s %s", task.Tid(), host, appName)
		}
		if err == nil {
			defer slave.Close()
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

	if slave == nil {
		cl.Log.WithFields(logrus.Fields{
			"session": task.Tid(),
			"error":   ErrAppUnavailable,
			"appname": appName,
		}).Error("unable to send task")
		return ErrAppUnavailable
	}

	raw, _ := task.Raw()
	var res string
	err = slave.Do("enqueue", "handleTask", raw).Wait(ctx, &res)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{
			"session": task.Tid(),
			"error":   err,
			"appname": appName,
			"host":    host,
		}).Errorf("task for group %s failed", task.Group())
		return err
	}

	cl.Log.WithFields(logrus.Fields{
		"session": task.Tid(),
		"appname": appName,
		"host":    host,
	}).Infof("task for group %s done: %s", task.Group(), res)
	return nil
}

func (cl *Client) doParsingTask(ctx context.Context, task tasks.ParsingTask, hosts []string) {
	if err := cl.doGeneralTask(ctx, common.PARSING, &task, hosts); err != nil {
		cl.clientStats.AddFailedParsing()
		return
	}
	cl.clientStats.AddSuccessParsing()
}

func (cl *Client) doAggregationHandler(ctx context.Context, task tasks.AggregationTask, hosts []string) {
	if err := cl.doGeneralTask(ctx, common.AGGREGATE, &task, hosts); err != nil {
		cl.clientStats.AddFailedAggregate()
		return
	}
	cl.clientStats.AddSuccessAggregate()
}

func getRandomHost(input []string) string {
	max := len(input)
	return input[rand.Intn(max)]
}

func PerformTask(ctx context.Context, app *cocaine.Service, payload []byte) (interface{}, error) {
	select {
	case res := <-app.Call("enqueue", "handleTask", payload):
		if res.Err() != nil {
			return nil, res.Err()
		}
		var i interface{}
		err := res.Extract(&i)
		return i, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
