package combainer

import (
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"strings"
	"sync"
	"time"

	"launchpad.net/goyaml"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/cfgwatcher"
)

type combainerMainCfg struct {
	Http_hand     string "HTTP_HAND"
	MinimumPeriod uint   "MINIMUM_PERIOD"
	CloudHosts    string "cloud"
}

type combainerLockserverCfg struct {
	Id      string   "app_id"
	Hosts   []string "host"
	Name    string   "name"
	timeout uint     "timeout"
}

type combainerConfig struct {
	Combainer struct {
		Main          combainerMainCfg       "Main"
		LockServerCfg combainerLockserverCfg "Lockserver"
	} "Combainer"
}

type sessionParams struct {
	ParsingTime time.Duration
	WholeTime   time.Duration
	PTasks      []common.ParsingTask
	AggTasks    []common.AggregationTask
}

type Client struct {
	Main       combainerMainCfg
	LSCfg      combainerLockserverCfg
	DLS        LockServer
	lockname   string
	cloudHosts []string
	clientStats

	// various periods and list of tasks
	sp *sessionParams
}

func (cs *clientStats) AddSuccess() {
	cs.Lock()
	cs.success++
	cs.Unlock()
}

func (cs *clientStats) AddFailed() {
	cs.Lock()
	cs.failed++
	cs.Unlock()
}

func (cs *clientStats) GetStats() (info *StatInfo) {
	cs.RLock()
	var success = cs.success
	var failed = cs.failed
	cs.RUnlock()
	info = &StatInfo{
		Success: success,
		Failed:  failed,
		Total:   success + failed,
	}
	return
}

// Public API

func NewClient(config string) (*Client, error) {
	// Read combaine.yaml
	data, err := ioutil.ReadFile(config)
	if err != nil {
		return nil, err
	}

	// Parse combaine.yaml
	var m combainerConfig
	err = goyaml.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}

	// Zookeeper hosts. Connect to Zookeeper
	hosts := m.Combainer.LockServerCfg.Hosts
	dls, err := NewLockServer(strings.Join(hosts, ","))
	if err != nil {
		return nil, err
	}

	cloudHosts, err := GetHosts(m.Combainer.Main.Http_hand, m.Combainer.Main.CloudHosts)
	if err != nil {
		return nil, err
	}
	return &Client{
		Main:       m.Combainer.Main,
		LSCfg:      m.Combainer.LockServerCfg,
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
	// tasks
	var p_tasks []common.ParsingTask
	var agg_tasks []common.AggregationTask

	// timeouts
	var parsingTime time.Duration
	var wholeTime time.Duration

	res, err := loadConfig(cl.lockname)
	if err != nil {
		LogInfo("Unable to load config %s", err)
		return
	}

	if res.MinimumPeriod > 0 {
		cl.Main.MinimumPeriod = res.MinimumPeriod
	}

	var metahost string
	if len(res.Metahost) != 0 {
		metahost = res.Metahost
	} else {
		metahost = res.Groups[0]
	}
	LogInfo("Metahost %s", metahost)
	// Make list of hosts
	var hosts []string
	for _, item := range res.Groups {
		if hosts_for_group, err := GetHosts(cl.Main.Http_hand, item); err != nil {
			LogInfo("Item %s, err %s", item, err)
		} else {
			hosts = append(hosts, hosts_for_group...)
		}
		LogInfo("Hosts: %s", hosts)
	}

	// Tasks for parsing
	//host_name, config_name, group_name, previous_time, current_time
	for _, host := range hosts {
		p_tasks = append(p_tasks, common.ParsingTask{
			Host:     host,
			Config:   cl.lockname,
			Group:    res.Groups[0],
			PrevTime: -1,
			CurrTime: -1,
			Id:       "",
			Metahost: metahost,
		})
	}

	//groupname, config_name, agg_config_name, previous_time, current_time
	for _, cfg := range res.AggConfigs {
		agg_tasks = append(agg_tasks, common.AggregationTask{
			Config:   cfg,
			PConfig:  cl.lockname,
			Group:    res.Groups[0],
			PrevTime: -1,
			CurrTime: -1,
			Id:       "",
			Metahost: metahost,
		})
	}

	parsingTime = time.Duration(float64(cl.Main.MinimumPeriod)*0.8) * time.Second
	wholeTime = time.Duration(cl.Main.MinimumPeriod) * time.Second

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

	watcher, err := cfgwatcher.NewSimpleCfgwatcher()
	if err != nil {
		LogInfo("Watcher error: %s", err)
		return
	}
	defer watcher.Close()

	watchChan, err := watcher.Watch(fmt.Sprintf("%s%s", CONFIGS_PARSING_PATH, cl.lockname))
	if err != nil {
		LogInfo("WatchChan error: %s", err)
		return

	}

	_observer.RegisterClient(cl, cl.lockname)
	defer _observer.UnregisterClient(cl.lockname)

	//Update session parametrs from config
	if err := cl.UpdateSessionParams(cl.lockname); err != nil {
		LogInfo("Error %s", err)
		return
	}

	if cl.sp == nil {
		LogInfo("Unable to update parametrs of session")
		return
	}

	// Dispatch
	var deadline time.Time
	var startTime time.Time
	var wg sync.WaitGroup

	for {
		// Start periodically
		startTime = time.Now()
		deadline = startTime.Add(cl.sp.ParsingTime)
		LogInfo("Start new iteration at %v", startTime)

		h := md5.New()
		io.WriteString(h, (fmt.Sprintf("%s%d%d", cl.lockname, startTime, deadline)))
		uniqueID := fmt.Sprintf("%x", h.Sum(nil))

		for i, task := range cl.sp.PTasks {
			// Description of task
			task.PrevTime = startTime.Unix()
			task.CurrTime = startTime.Add(cl.sp.WholeTime).Unix()
			task.Id = uniqueID

			LogInfo("%s Send task number %d to parsing %v", uniqueID, i+1, task)
			go cl.parsingTaskHandler(task, &wg, deadline)
			wg.Add(1)
		}
		wg.Wait()
		LogInfo("%s Parsing finished", uniqueID)

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
		case <-lockpoller: // Lock
			LogInfo("%s do exit", uniqueID)
			return
		case <-time.After(deadline.Sub(time.Now())):
			LogInfo("%s Go to the next iteration", uniqueID)
			select {
			case err := <-watchChan:
				if err != nil {
					LogInfo("Watch channel error %s", err)
					return
				}
				cl.UpdateSessionParams(cl.lockname)
			default:
			}
		}
	}
}

func (cl *Client) parsingTaskHandler(task common.ParsingTask, wg *sync.WaitGroup, deadline time.Time) {
	defer (*wg).Done()
	limit := deadline.Sub(time.Now())

	var app *cocaine.Service
	var err error
	for deadline.After(time.Now()) {
		host := fmt.Sprintf("%s:10053", cl.getRandomHost())
		app, err = cocaine.NewService(common.PARSING, host)
		if err == nil {
			defer app.Close()
			LogDebug("%s Host: %s", task.Id, host)
			break
		}
		time.Sleep(200 * time.Microsecond)
	}

	if app == nil {
		LogErr("Unable to send task %s. Application is unavailable", task.Id)
		cl.clientStats.AddFailed()
		return
	}

	raw, _ := common.Pack(task)
	select {
	case <-time.After(limit):
		LogErr("Task %s has been late\n", task.Id)
		cl.clientStats.AddFailed()
	case res := <-app.Call("enqueue", "handleTask", raw):
		if res.Err() != nil {
			LogErr("%s Parsing task for host %s failed %v", task.Id, task.Host, res.Err())
		} else {
			LogWarning("%s Parsing task for host %s completed successfully", task.Id, task.Host)
		}
		cl.clientStats.AddSuccess()
	}
}

func (cl *Client) aggregationTaskHandler(task common.AggregationTask, wg *sync.WaitGroup, deadline time.Time) {
	defer (*wg).Done()
	limit := deadline.Sub(time.Now())

	var app *cocaine.Service
	var err error
	for deadline.After(time.Now()) {
		host := fmt.Sprintf("%s:10053", cl.getRandomHost())
		app, err = cocaine.NewService(common.AGGREGATE, host)
		if err == nil {
			defer app.Close()
			LogDebug("%s Host: %s", task.Id, host)
			break
		}
		time.Sleep(time.Millisecond * 300)
	}

	if app == nil {
		cl.clientStats.AddFailed()
		LogErr("Unable to send aggregate task %s. Application is unavailable", task.Id)
		return
	}

	raw, _ := common.Pack(task)
	select {
	case <-time.After(limit):
		LogErr("Task %s has been late", task.Id)
		cl.clientStats.AddFailed()
	case res := <-app.Call("enqueue", "handleTask", raw):
		if res.Err() != nil {
			LogErr("%s Aggreagation task for group %s failed %v", task.Id, task.Group, res.Err())
		} else {
			LogWarning("%s Aggregation task for group %s completed successfully", task.Id, task.Group)
		}
		cl.clientStats.AddSuccess()
	}
}

func (cl *Client) getRandomHost() string {
	max := len(cl.cloudHosts)
	return cl.cloudHosts[rand.Intn(max)]
}

// Private API
func (cl *Client) acquireLock() chan bool {
	for _, i := range getParsings() {
		lockname := fmt.Sprintf("/%s/%s", cl.LSCfg.Id, i)
		poller := cl.DLS.AcquireLock(lockname)
		if poller != nil {
			cl.lockname = i
			return poller
		}
	}
	return nil
}
