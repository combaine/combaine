package combainer

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	// "github.com/howeyc/fsnotify"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/configs"
	"github.com/noxiouz/Combaine/common/tasks"
)

type sessionParams struct {
	ParsingTime time.Duration
	WholeTime   time.Duration
	PTasks      []tasks.ParsingTask
	AggTasks    []tasks.AggregationTask
}

type clientStats struct {
	sync.RWMutex
	successParsing   int
	failedParsing    int
	successAggregate int
	failedAggregate  int
	last             int64
}

type Client struct {
	Repository configs.Repository
	Config     configs.CombainerConfig
	DLS        LockServer
	lockname   string
	cloudHosts []string
	clientStats

	// various periods and list of tasks
	sp *sessionParams
}

func (cs *clientStats) AddSuccessParsing() {
	cs.Lock()
	cs.successParsing++
	cs.last = time.Now().Unix()
	cs.Unlock()
}

func (cs *clientStats) AddFailedParsing() {
	cs.Lock()
	cs.failedParsing++
	cs.last = time.Now().Unix()
	cs.Unlock()
}

func (cs *clientStats) AddSuccessAggregate() {
	cs.Lock()
	cs.successAggregate++
	cs.last = time.Now().Unix()
	cs.Unlock()
}

func (cs *clientStats) AddFailedAggregate() {
	cs.Lock()
	cs.failedAggregate++
	cs.last = time.Now().Unix()
	cs.Unlock()
}

func (cs *clientStats) GetStats() (info *StatInfo) {
	cs.RLock()
	// var success = cs.success
	// var failed = cs.failed
	defer cs.RUnlock()
	info = &StatInfo{
		ParsingSuccess:   cs.successParsing,
		ParsingFailed:    cs.failedParsing,
		ParsingTotal:     cs.successParsing + cs.failedParsing,
		AggregateSuccess: cs.successAggregate,
		AggregateFailed:  cs.failedAggregate,
		AggregateTotal:   cs.successAggregate + cs.failedAggregate,
		Heartbeated:      cs.last,
	}
	return
}

func NewClient(config configs.CombainerConfig, repo configs.Repository) (*Client, error) {
	// Zookeeper hosts. Connect to Zookeeper
	// TBD: It's better to pass config as is of create lockserver outside of client
	dls, err := NewLockServer(strings.Join(config.LockServerSection.Hosts, ","))
	if err != nil {
		return nil, err
	}

	// Get list of Combaine hosts
	cloudHosts, err := GetHosts(config.MainSection.Http_hand, config.MainSection.CloudGroup)
	if err != nil {
		return nil, err
	}

	return &Client{
		Repository: repo,
		Config:     config,
		DLS:        *dls,
		lockname:   "",
		cloudHosts: cloudHosts,
		sp:         nil,
	}, nil
}

func (cl *Client) Close() {
	cl.DLS.Close()
}

func (cl *Client) UpdateSessionParams(config string) (err error) {
	LogInfo("Updating session parametrs")
	var (
		// tasks
		p_tasks   []tasks.ParsingTask
		agg_tasks []tasks.AggregationTask

		// timeouts
		parsingTime time.Duration
		wholeTime   time.Duration
	)

	parsingConfig, err := cl.Repository.GetParsingConfig(cl.lockname)
	if err != nil {
		LogErr("Unable to load config %s", err)
		return
	}

	if parsingConfig.IterationDuration > 0 {
		cl.Config.MainSection.IterationDuration = parsingConfig.IterationDuration
	}

	// common.MapUpdate(&(combainerCfg.CloudCfg.DF), &(cfg.DF))
	// cfg.DF = combainerCfg.CloudCfg.DF
	common.PluginConfigsUpdate(&cl.Config.CloudSection.DataFetcher, &parsingConfig.DataFetcher)
	parsingConfig.DataFetcher = cl.Config.CloudSection.DataFetcher

	LogInfo("Updating config: group %s, metahost %s",
		parsingConfig.GetGroup(), parsingConfig.GetMetahost())
	// Make list of hosts
	var hosts []string
	for _, item := range parsingConfig.Groups {
		if hosts_for_group, err := GetHosts(cl.Config.MainSection.Http_hand, item); err != nil {
			LogInfo("Item %s, err %s", item, err)
		} else {
			hosts = append(hosts, hosts_for_group...)
		}
		LogInfo("Hosts: %s", hosts)
	}

	aggregationConfigs := make(map[string]configs.AggregationConfig)
	for _, name := range parsingConfig.AggConfigs {
		content, err := cl.Repository.GetAggregationConfig(name)
		if err != nil {
			// It seems better to throw error here instead of
			// going data processing on without config
			LogErr("Unable to read aggregation config %s, %s", name, err)
			return err
		}
		aggregationConfigs[name] = content
	}

	// Tasks for parsing
	for _, host := range hosts {
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
		})
	}

	parsingTime, wholeTime = GenerateSessionTimeFrame(cl.Config.MainSection.IterationDuration)

	sp := sessionParams{
		ParsingTime: parsingTime,
		WholeTime:   wholeTime,
		PTasks:      p_tasks,
		AggTasks:    agg_tasks,
	}

	LogInfo("Session parametrs have been updated successfully. %v", sp)
	cl.sp = &sp
	return nil
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
		//Update session parametrs from config
		if err := cl.UpdateSessionParams(cl.lockname); err != nil {
			LogInfo("Error %s", err)
			return
		}

		if cl.sp == nil {
			LogInfo("Unable to update parametrs of session")
			return
		}

		// Start periodically
		startTime = time.Now()
		deadline = startTime.Add(cl.sp.ParsingTime)

		// Generate session unique ID
		uniqueID := GenerateSessionId(cl.lockname, &startTime, &deadline)
		LogInfo("%s Start new iteration.", uniqueID)

		// Parsing phase
		for i, task := range cl.sp.PTasks {
			// Description of task
			task.PrevTime = startTime.Unix()
			task.CurrTime = startTime.Add(cl.sp.WholeTime).Unix()
			task.Id = uniqueID

			LogInfo("%s Send task number %d to parsing %v", uniqueID, i+1, task)
			wg.Add(1)
			go cl.parsingTaskHandler(task, &wg, deadline)
		}
		wg.Wait()
		LogInfo("%s Parsing finished", uniqueID)

		// Aggregation phase
		deadline = startTime.Add(cl.sp.WholeTime)
		for i, task := range cl.sp.AggTasks {
			task.PrevTime = startTime.Unix()
			task.CurrTime = startTime.Add(cl.sp.WholeTime).Unix()
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
		} else {
			LogWarning("%s unable to connect to application %s %s %s", task.Id, common.PARSING, host, err)
		}
		time.Sleep(200 * time.Microsecond)
	}

	if app == nil {
		LogErr("Unable to send task %s. Application is unavailable", task.Id)
		cl.clientStats.AddFailedParsing()
		return
	}

	raw, _ := common.Pack(task)
	select {
	case <-time.After(limit):
		LogErr("Task %s has been late\n", task.Id)
		cl.clientStats.AddFailedParsing()
	case res := <-app.Call("enqueue", "handleTask", raw):
		if res.Err() != nil {
			LogErr("%s Parsing task for host %s failed %v", task.Id, task.Host, res.Err())
		} else {
			LogInfo("%s Parsing task for host %s completed successfully", task.Id, task.Host)
		}
		cl.clientStats.AddSuccessParsing()
	}
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
		} else {
			LogWarning("%s unable to connect to application %s %s %s", task.Id, common.AGGREGATE, host, err)
		}
		time.Sleep(time.Millisecond * 100)
	}

	if app == nil {
		cl.clientStats.AddFailedAggregate()
		LogErr("Unable to send aggregate task %s. Application is unavailable", task.Id)
		return
	}

	raw, _ := common.Pack(task)
	select {
	case <-time.After(limit):
		LogErr("Task %s has been late", task.Id)
		cl.clientStats.AddFailedAggregate()
	case res := <-app.Call("enqueue", "handleTask", raw):
		if res.Err() != nil {
			LogErr("%s Aggreagation task for group %s failed %v", task.Id, task.ParsingConfig.GetGroup(), res.Err())
		} else {
			LogInfo("%s Aggregation task for group %s completed successfully", task.Id, task.ParsingConfig.GetGroup())
		}
		cl.clientStats.AddSuccessAggregate()
	}
}

func (cl *Client) getRandomHost() string {
	max := len(cl.cloudHosts)
	return cl.cloudHosts[rand.Intn(max)]
}

// Private API
func (cl *Client) acquireLock() chan bool {
	for _, i := range cl.Repository.ListParsingConfigs() {
		lockname := fmt.Sprintf("/%s/%s", cl.Config.LockServerSection.Id, i)
		poller := cl.DLS.AcquireLock(lockname)
		if poller != nil {
			cl.lockname = i
			return poller
		}
	}
	return nil
}
