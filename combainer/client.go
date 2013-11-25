package combainer

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strings"
	"time"

	"launchpad.net/goyaml"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/noxiouz/Combaine/common"
)

type combainerMainCfg struct {
	Http_hand       string "HTTP_HAND"
	MaxPeriod       uint   "MAXIMUM_PERIOD"
	MaxAttemps      uint   "MAX_ATTEMPS"
	MaxRespWaitTime uint   "MAX_RESP_WAIT_TIME"
	MinimumPeriod   uint   "MINIMUM_PERIOD"
	CloudHosts      string "cloud"
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

type Client struct {
	Main       combainerMainCfg
	LSCfg      combainerLockserverCfg
	DLS        LockServer
	lockname   string
	cloudHosts []string
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
	log.Println(cloudHosts)
	return &Client{
		Main:       m.Combainer.Main,
		LSCfg:      m.Combainer.LockServerCfg,
		DLS:        *dls,
		lockname:   "",
		cloudHosts: cloudHosts,
	}, nil
}

func (cl *Client) Close() {
	cl.DLS.Close()
}

func (cl *Client) Dispatch() {
	defer cl.Close()
	lockpoller := cl.acquireLock()
	if lockpoller != nil {
		log.Println("Acquire Lock", cl.lockname)
	} else {
		return
	}

	var p_tasks []common.ParsingTask
	var agg_tasks []common.AggregationTask
	if res, err := loadConfig(cl.lockname); err != nil {
		log.Println(err)
		return
	} else {
		// Make list of hosts
		var hosts []string
		for _, item := range res.Groups {
			if hosts_for_group, err := GetHosts(cl.Main.Http_hand, item); err != nil {
				log.Println(item, err)
			} else {
				hosts = append(hosts, hosts_for_group...)
			}
			log.Println(hosts)

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
			})
		}
	}

	PARSING_TIME := time.Duration(float64(cl.Main.MinimumPeriod)*0.6) * time.Second
	WHOLE_TIME := time.Duration(cl.Main.MinimumPeriod) * time.Second

	countOfParsingTasks := len(p_tasks)
	countOfAggTasks := len(agg_tasks)

	// Dispatch
	ticker := time.NewTimer(time.Millisecond * 1)
	var par_done chan cocaine.ServiceResult
	var agg_done chan cocaine.ServiceResult
	var tasks_done int = 0
	var deadline time.Time
	var startTime time.Time

	for {
		select {
		// Start periodically
		case startTime = <-ticker.C:
			deadline = startTime.Add(PARSING_TIME)
			agg_done = nil
			log.Println(startTime)
			tasks_done = 0
			// Start next iteration after WHOLE_TIME
			// despite of completed tasks
			ticker.Reset(WHOLE_TIME)
			par_done = make(chan cocaine.ServiceResult)
			for i, task := range p_tasks {
				// Description of task
				task.PrevTime = startTime.Unix()
				task.CurrTime = startTime.Add(WHOLE_TIME).Unix()
				task.Id = fmt.Sprintf("%v", task)

				log.Println("Send task number ", i, task)
				go cl.parsingTaskHandler(task, par_done, deadline)
			}
		// Collect parsing result
		case res := <-par_done:
			var r string
			if res.Err() == nil {
				res.Extract(&r)
				log.Printf("Par %v", r)
			} else {
				log.Printf("Erorr %s", res.Err())
			}
			tasks_done += 1
			if tasks_done == countOfParsingTasks {
				par_done = nil
				agg_done = make(chan cocaine.ServiceResult)
				tasks_done = 0
				deadline = startTime.Add(WHOLE_TIME)
				for i, task := range agg_tasks {
					log.Println("Send task number ", i, task)
					task.PrevTime = startTime.Unix()
					task.CurrTime = startTime.Add(WHOLE_TIME).Unix()
					go cl.aggregationTaskHandler(task, agg_done, deadline)
				}
			}
		// Collect agg results
		case res := <-agg_done:
			log.Println("Agg", res)
			tasks_done += 1
			if tasks_done == countOfAggTasks {
				agg_done = nil
				tasks_done = 0
			}
		case <-lockpoller: // Lock
			log.Println("do exit")
			return
		case <-time.After(time.Second * 1):
			log.Println("Heartbeat")
		}
	}
}

func (cl *Client) parsingTaskHandler(task common.ParsingTask, answer chan cocaine.ServiceResult, deadline time.Time) {
	limit := deadline.Sub(time.Now())

	var app *cocaine.Service
	var err error
	for deadline.After(time.Now()) {
		host := fmt.Sprintf("%s:10053", cl.getRandomHost())
		app, err = cocaine.NewService(common.PARSING, host)
		if err == nil {
			defer app.Close()
			break
		}
		time.Sleep(200 * time.Microsecond)
	}

	if app == nil {
		return
	}

	raw, _ := common.Pack(task)
	select {
	case <-time.After(limit):
		log.Printf("Task %s has been late\n", task.Id)
	case res := <-app.Call("enqueue", "handleTask", raw):
		log.Printf("Receive result %v\n", res)
		answer <- res
	}
}

func (cl *Client) aggregationTaskHandler(task common.AggregationTask, answer chan cocaine.ServiceResult, deadline time.Time) {
	limit := deadline.Sub(time.Now())

	var app *cocaine.Service
	var err error
	for deadline.After(time.Now()) {
		host := fmt.Sprintf("%s:10053", cl.getRandomHost())
		app, err = cocaine.NewService(common.AGGREGATE, host)
		if err == nil {
			defer app.Close()
			break
		}
		time.Sleep(time.Millisecond * 300)
	}

	if app == nil {
		return
	}

	raw, _ := common.Pack(task)
	select {
	case <-time.After(limit):
		log.Println("Task %s has been late", task.Id)
	case res := <-app.Call("enqueue", "handleTask", raw):
		log.Println("Aggregate done ", res.Err())
		answer <- res
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
