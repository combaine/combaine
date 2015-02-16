package combainer

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/configs"
	"github.com/noxiouz/Combaine/common/hosts"
	"github.com/noxiouz/Combaine/common/tasks"
)

var (
	ErrAppUnavailable = fmt.Errorf("Application is unavailable")
)

type sessionParams struct {
	ParsingTime time.Duration
	WholeTime   time.Duration
	PTasks      []tasks.ParsingTask
	AggTasks    []tasks.AggregationTask
}

type Client struct {
	Repository configs.Repository
	*Context
	clientStats
}

func NewClient(context *Context, repo configs.Repository) (*Client, error) {
	if context.Hosts == nil {
		err := fmt.Errorf("Unable to create new client: Hosts delegate must be specified")
		LogErr(err.Error())
		return nil, err
	}

	cl := &Client{
		Repository: repo,
		Context:    context,
	}
	return cl, nil
}

func (cl *Client) UpdateSessionParams(config string) (sp *sessionParams, err error) {
	LogInfo("Updating session parametrs")
	var (
		// tasks
		p_tasks   []tasks.ParsingTask
		agg_tasks []tasks.AggregationTask

		// timeouts
		parsingTime time.Duration
		wholeTime   time.Duration
	)

	encodedParsingConfig, err := cl.Repository.GetParsingConfig(config)
	if err != nil {
		LogErr("Unable to load config %s", err)
		return nil, err
	}

	var parsingConfig configs.ParsingConfig
	if err := encodedParsingConfig.Decode(&parsingConfig); err != nil {
		LogErr("Unable to decode parsingConfig: %s", err)
		return nil, err
	}

	cfg := cl.Repository.GetCombainerConfig()
	parsingConfig.UpdateByCombainerConfig(&cfg)
	aggregationConfigs, err := GetAggregationConfigs(cl.Repository, &parsingConfig)
	if err != nil {
		LogErr("Unable to read aggregation configs: %s", err)
		return nil, err
	}

	LogInfo("Updating config: group %s, metahost %s",
		parsingConfig.GetGroup(), parsingConfig.GetMetahost())

	hostFetcher, err := LoadHostFetcher(cl.Context, parsingConfig.HostFetcher)
	if err != nil {
		LogErr("Unable to construct SimpleFetcher: %s", err)
		return
	}

	allHosts := make(hosts.Hosts)
	for _, item := range parsingConfig.Groups {
		hosts_for_group, err := hostFetcher.Fetch(item)
		if err != nil {
			LogInfo("Unable to get hosts for group %s: %s", item, err)
			continue
		}

		allHosts.Merge(&hosts_for_group)
	}
	listOfHosts := allHosts.AllHosts()
	LogInfo("Hosts: %s", listOfHosts)
	if len(listOfHosts) == 0 {
		return nil, fmt.Errorf("No hosts in given groups")
	}

	// Tasks for parsing
	for _, host := range listOfHosts {
		p_tasks = append(p_tasks, tasks.ParsingTask{
			CommonTask:         tasks.EmptyCommonTask,
			Host:               host,
			ParsingConfigName:  config,
			ParsingConfig:      parsingConfig,
			AggregationConfigs: *aggregationConfigs,
		})
	}

	for _, name := range parsingConfig.AggConfigs {
		agg_tasks = append(agg_tasks, tasks.AggregationTask{
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
		PTasks:      p_tasks,
		AggTasks:    agg_tasks,
	}

	LogInfo("Session parametrs have been updated successfully. %v", sp)
	return sp, nil
}

func (cl *Client) Dispatch(parsingConfigName string, uniqueID string, shouldWait bool) error {
	_observer.RegisterClient(cl, parsingConfigName)
	defer _observer.UnregisterClient(parsingConfigName)

	var deadline, startTime time.Time
	var wg sync.WaitGroup

	sessionParameters, err := cl.UpdateSessionParams(parsingConfigName)
	if err != nil {
		LogInfo("Error %s", err)
		return err
	}

	startTime = time.Now()
	deadline = startTime.Add(sessionParameters.ParsingTime)

	if uniqueID == "" {
		// Generate session unique ID if it hasn't been specified
		uniqueID = GenerateSessionId(parsingConfigName, &startTime, &deadline)
		LogInfo("%s ID has been generated", uniqueID)
	}
	LogInfo("%s Start new iteration.", uniqueID)

	hosts, err := cl.Context.Hosts()
	if err != nil || len(hosts) == 0 {
		LogErr("%s unable to get (or empty) the list of the cloud hosts: %s", uniqueID, err)
		return err
	}

	// Parsing phase
	for i, task := range sessionParameters.PTasks {
		// Description of task
		task.PrevTime = startTime.Unix()
		task.CurrTime = startTime.Add(sessionParameters.WholeTime).Unix()
		task.CommonTask.Id = uniqueID

		LogInfo("%s Send task number %d to parsing %v", uniqueID, i+1, task)
		wg.Add(1)
		go cl.doParsingTask(&task, &wg, deadline, hosts)
	}
	wg.Wait()
	LogInfo("%s Parsing finished", uniqueID)

	// Aggregation phase
	deadline = startTime.Add(sessionParameters.WholeTime)
	for i, task := range sessionParameters.AggTasks {
		task.PrevTime = startTime.Unix()
		task.CurrTime = startTime.Add(sessionParameters.WholeTime).Unix()
		task.CommonTask.Id = uniqueID
		LogInfo("%s Send task number %d to aggregate %v", uniqueID, i+1, task)
		wg.Add(1)
		go cl.doAggregationHandler(&task, &wg, deadline, hosts)
	}
	wg.Wait()
	LogInfo("%s Aggregation finished", uniqueID)

	// Wait for next iteration
	if shouldWait {
		time.Sleep(deadline.Sub(time.Now()))
	}
	LogInfo("%s Go to the next iteration", uniqueID)
	return nil
}

type ResolveInfo struct {
	App *cocaine.Service
	Err error
}

func Resolve(appname, endpoint string) <-chan ResolveInfo {
	res := make(chan ResolveInfo)
	go func() {
		app, err := cocaine.NewService(appname, endpoint)
		select {
		case res <- ResolveInfo{
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
		case r := <-Resolve(appName, host):
			err, app = r.Err, r.App
		case <-time.After(1 * time.Second):
			err = fmt.Errorf("service resolvation was timeouted %s %s %s", task.Id(), host, appName)
		}
		if err == nil {
			defer app.Close()
			LogDebug("%s Host: %s", task.Id, host)
			break
		}

		LogWarning("%s unable to connect to application %s %s %s", task.Id(), appName, host, err)
		time.Sleep(200 * time.Microsecond)
	}

	if app == nil {
		LogErr("Unable to send task %s. Application is unavailable", task.Id())
		return ErrAppUnavailable
	}

	raw, _ := task.Raw()
	res, err := PerformTask(app, raw, limit)
	if err != nil {
		LogErr("%s %s task for group %s %s failed: %s", appName, task.Id, task.Group(), host, err)
		return err
	}
	LogInfo("%s %s task for group %s %s done: %s", appName, task.Id, task.Group(), host, res)
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
	case <-time.After(limit):
		return nil, fmt.Errorf("timeout")
	case res := <-app.Call("enqueue", "handleTask", payload):
		if res.Err() != nil {
			return nil, res.Err()
		}
		var i interface{}
		err := res.Extract(&i)
		return i, err
	}
	return nil, nil
}
