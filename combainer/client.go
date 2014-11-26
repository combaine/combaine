package combainer

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/configs"
	"github.com/noxiouz/Combaine/common/hosts"
	"github.com/noxiouz/Combaine/common/tasks"
)

type sessionParams struct {
	ParsingTime time.Duration
	WholeTime   time.Duration
	PTasks      []tasks.ParsingTask
	AggTasks    []tasks.AggregationTask
}

type Client struct {
	Repository configs.Repository
	Config     configs.CombainerConfig
	DLS        LockServer
	lockname   string
	cloudHosts []string
	*Context
	clientStats
}

func NewClient(context *Context, config configs.CombainerConfig, repo configs.Repository) (*Client, error) {
	// Zookeeper hosts. Connect to Zookeeper
	// TBD: It's better to pass config as is of create lockserver outside of client
	dls, err := NewLockServer(strings.Join(config.LockServerSection.Hosts, ","))
	if err != nil {
		return nil, err
	}

	s, err := NewSimpleFetcher(context, config.CloudSection.HostFetcher)
	if err != nil {
		return nil, err
	}

	// Get Combaine hosts
	cloudHosts, err := s.Fetch(config.MainSection.CloudGroup)
	if err != nil {
		return nil, err
	}

	cl := &Client{
		Repository: repo,
		Config:     config,
		DLS:        *dls,
		lockname:   "",
		cloudHosts: cloudHosts.AllHosts(),
		Context:    context,
	}

	return cl, nil
}

func (cl *Client) Close() {
	cl.DLS.Close()
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

	encodedParsingConfig, err := cl.Repository.GetParsingConfig(cl.lockname)
	if err != nil {
		LogErr("Unable to load config %s", err)
		return nil, err
	}

	var parsingConfig configs.ParsingConfig
	if err := encodedParsingConfig.Decode(&parsingConfig); err != nil {
		LogErr("Unable to decode parsingConfig: %s", err)
		return nil, err
	}

	if parsingConfig.IterationDuration > 0 {
		cl.Config.MainSection.IterationDuration = parsingConfig.IterationDuration
	}

	common.PluginConfigsUpdate(&cl.Config.CloudSection.DataFetcher, &parsingConfig.DataFetcher)
	parsingConfig.DataFetcher = cl.Config.CloudSection.DataFetcher
	common.PluginConfigsUpdate(&cl.Config.CloudSection.HostFetcher, &parsingConfig.HostFetcher)
	parsingConfig.HostFetcher = cl.Config.CloudSection.HostFetcher

	LogInfo("Updating config: group %s, metahost %s",
		parsingConfig.GetGroup(), parsingConfig.GetMetahost())

	hostFetcher, err := NewSimpleFetcher(cl.Context, parsingConfig.HostFetcher)
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

	aggregationConfigs := make(map[string]configs.AggregationConfig)
	for _, name := range parsingConfig.AggConfigs {
		content, err := cl.Repository.GetAggregationConfig(name)
		if err != nil {
			// It seems better to throw error here instead of
			// going data processing on without config
			LogErr("Unable to read aggregation config %s, %s", name, err)
			return nil, err
		}

		var aggConfig configs.AggregationConfig
		if err := content.Decode(&aggConfig); err != nil {
			LogErr("Unable to decode aggConfig: %s", err)
			return nil, err
		}
		aggregationConfigs[name] = aggConfig
	}

	// Tasks for parsing
	for _, host := range listOfHosts {
		p_tasks = append(p_tasks, tasks.ParsingTask{
			CommonTask:         tasks.EmptyCommonTask,
			Host:               host,
			ParsingConfigName:  cl.lockname,
			ParsingConfig:      parsingConfig,
			AggregationConfigs: aggregationConfigs,
		})
	}

	for _, name := range parsingConfig.AggConfigs {
		agg_tasks = append(agg_tasks, tasks.AggregationTask{
			CommonTask:        tasks.EmptyCommonTask,
			Config:            name,
			ParsingConfigName: cl.lockname,
			ParsingConfig:     parsingConfig,
			AggregationConfig: aggregationConfigs[name],
			Hosts:             allHosts,
		})
	}

	parsingTime, wholeTime = GenerateSessionTimeFrame(cl.Config.MainSection.IterationDuration)

	sp = &sessionParams{
		ParsingTime: parsingTime,
		WholeTime:   wholeTime,
		PTasks:      p_tasks,
		AggTasks:    agg_tasks,
	}

	LogInfo("Session parametrs have been updated successfully. %v", sp)
	return sp, nil
}

func (cl *Client) Dispatch() {
	defer cl.Close()

	lockpoller := cl.acquireLock()
	if lockpoller != nil {
		LogInfo("Acquire Lock %s", cl.lockname)
	} else {
		return
	}

	_observer.RegisterClient(cl, cl.lockname)
	defer _observer.UnregisterClient(cl.lockname)

	// Dispatch
	var (
		deadline  time.Time
		startTime time.Time
		wg        sync.WaitGroup
	)

	for {
		sessionParameters, err := cl.UpdateSessionParams(cl.lockname)
		if err != nil {
			LogInfo("Error %s", err)
			return
		}

		startTime = time.Now()
		deadline = startTime.Add(sessionParameters.ParsingTime)

		// Generate session unique ID
		uniqueID := GenerateSessionId(cl.lockname, &startTime, &deadline)
		LogInfo("%s Start new iteration.", uniqueID)

		// Parsing phase
		for i, task := range sessionParameters.PTasks {
			// Description of task
			task.PrevTime = startTime.Unix()
			task.CurrTime = startTime.Add(sessionParameters.WholeTime).Unix()
			task.Id = uniqueID

			LogInfo("%s Send task number %d to parsing %v", uniqueID, i+1, task)
			wg.Add(1)
			go cl.parsingTaskHandler(task, &wg, deadline)
		}
		wg.Wait()
		LogInfo("%s Parsing finished", uniqueID)

		// Aggregation phase
		deadline = startTime.Add(sessionParameters.WholeTime)
		for i, task := range sessionParameters.AggTasks {
			task.PrevTime = startTime.Unix()
			task.CurrTime = startTime.Add(sessionParameters.WholeTime).Unix()
			task.Id = uniqueID
			LogInfo("%s Send task number %d to aggregate %v", uniqueID, i+1, task)
			wg.Add(1)
			go cl.aggregationTaskHandler(task, &wg, deadline)
		}
		wg.Wait()
		LogInfo("%s Aggregation finished", uniqueID)

		select {
		// Does lock exist?
		case <-lockpoller: // Lock
			LogInfo("%s Drop lock %s", uniqueID, cl.lockname)
			return

		// Wait for next iteration
		case <-time.After(deadline.Sub(time.Now())):
		}
		LogInfo("%s Go to the next iteration", uniqueID)
	}
}

type ResolveInfo struct {
	App *cocaine.Service
	Err error
}

func Resolve(appname, endpoint string) <-chan ResolveInfo {
	res := make(chan ResolveInfo, 1)
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

func (cl *Client) parsingTaskHandler(task tasks.ParsingTask, wg *sync.WaitGroup, deadline time.Time) {
	defer (*wg).Done()
	limit := deadline.Sub(time.Now())

	var app *cocaine.Service
	var err error
	for deadline.After(time.Now()) {
		host := fmt.Sprintf("%s:10053", cl.getRandomHost())
		select {
		case r := <-Resolve(common.PARSING, host):
			err = r.Err
			app = r.App
		case <-time.After(1 * time.Second):
			err = fmt.Errorf("service resolvation was timeouted %s %s %s", task.Id, host, common.PARSING)
		}
		if err == nil {
			defer app.Close()
			LogDebug("%s Host: %s", task.Id, host)
			break
		}

		LogWarning("%s unable to connect to application %s %s %s", task.Id, common.PARSING, host, err)
		time.Sleep(200 * time.Microsecond)
	}

	if app == nil {
		LogErr("Unable to send task %s. Application is unavailable", task.Id)
		cl.clientStats.AddFailedParsing()
		return
	}

	raw, _ := common.Pack(task)
	if res, err := PerformTask(app, raw, limit); err != nil {
		LogErr("%s Parsing task for group %s failed: %s", task.Id, task.ParsingConfig.GetGroup(), err)
		cl.clientStats.AddFailedParsing()
		return
	} else {
		LogErr("%s Parsing task for group %s finished good: %s", task.Id, task.ParsingConfig.GetGroup(), res)
	}
	cl.clientStats.AddSuccessParsing()
}

func (cl *Client) aggregationTaskHandler(task tasks.AggregationTask, wg *sync.WaitGroup, deadline time.Time) {
	defer (*wg).Done()
	limit := deadline.Sub(time.Now())

	var app *cocaine.Service
	var err error
	for deadline.After(time.Now()) {
		host := fmt.Sprintf("%s:10053", cl.getRandomHost())
		select {
		case r := <-Resolve(common.AGGREGATE, host):
			err = r.Err
			app = r.App
		case <-time.After(1 * time.Second):
			err = fmt.Errorf("service resolvation was timeouted %s %s %s", task.Id, host, common.AGGREGATE)
		}

		if err == nil {
			defer app.Close()
			LogDebug("%s Host: %s", task.Id, host)
			break
		}

		LogWarning("%s unable to connect to application %s %s %s", task.Id, common.AGGREGATE, host, err)
		time.Sleep(time.Millisecond * 100)
	}

	if app == nil {
		cl.clientStats.AddFailedAggregate()
		LogErr("Unable to send aggregate task %s. Application is unavailable", task.Id)
		return
	}

	raw, _ := common.Pack(task)
	if res, err := PerformTask(app, raw, limit); err != nil {
		LogErr("%s Aggreagation task for group %s failed: %s", task.Id, task.ParsingConfig.GetGroup(), err)
		cl.clientStats.AddFailedAggregate()
		return
	} else {
		LogErr("%s Aggreagation task for group %s finished good: %s", task.Id, task.ParsingConfig.GetGroup(), res)
	}
	cl.clientStats.AddSuccessAggregate()
}

func (cl *Client) getRandomHost() string {
	max := len(cl.cloudHosts)
	return cl.cloudHosts[rand.Intn(max)]
}

// Private API
func (cl *Client) acquireLock() chan bool {
	parsingListing, _ := cl.Repository.ListParsingConfigs()
	for _, i := range parsingListing {
		lockname := fmt.Sprintf("/%s/%s", cl.Config.LockServerSection.Id, i)
		poller := cl.DLS.AcquireLock(lockname)
		if poller != nil {
			cl.lockname = i
			return poller
		}
	}
	return nil
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
