package combainer

import (
	"fmt"
	"io/ioutil"
	"launchpad.net/goyaml"
	"log"
	"math/rand"
	"strings"
	"time"
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

type TaskResult struct {
	success bool
	reason  string
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

	// Zookeeper hosts. Commect to Zookeeper
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

	lockpoller := cl.acquireLock()
	if lockpoller != nil {
		log.Println("Acquire Lock", cl.lockname)
	} else {
		log.Println("There are no free locks")
		return
	}

	p_tasks := []map[string]string{}
	agg_tasks := []map[string]string{}
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
			p_tasks = append(p_tasks, map[string]string{
				"target":    host,
				"groupname": res.Groups[0],
				"config":    cl.lockname,
			})
		}
		//groupname, config_name, agg_config_name, previous_time, current_time
		for _, cfg := range res.AggConfigs {
			agg_tasks = append(agg_tasks, map[string]string{
				"target":    cfg,
				"groupname": res.Groups[0],
				"config":    cl.lockname,
			})
		}
	}

	handleTask := func(task map[string]string, answer chan TaskResult, deadline time.Time) {
		limit := deadline.Sub(time.Now())
		// Owner
		// Task description and hash
		log.Println("time limit ", limit) // MORE INFO
		select {
		case <-time.After(limit):
			log.Println("Timeout")
		case <-time.After(time.Second * time.Duration(rand.Intn(5))):
			if deadline.Sub(time.Now()).Nanoseconds() > 0 {
				log.Println("Send result")
				answer <- TaskResult{true, "OK"}
			}
		}
	}

	PARSING_TIME := time.Duration(float64(cl.Main.MinimumPeriod)*0.6) * time.Second
	WHOLE_TIME := time.Duration(cl.Main.MinimumPeriod) * time.Second

	countOfParsingTasks := len(p_tasks)
	countOfAggTasks := len(agg_tasks)

	// Dispatch
	ticker := time.NewTimer(time.Millisecond * 1)
	var par_done chan TaskResult
	var agg_done chan TaskResult
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
			par_done = make(chan TaskResult)
			for i, task := range p_tasks {
				// Description of task
				log.Println("Send task number ", i, task)
				go handleTask(task, par_done, deadline)
			}
		// Collect parsing result
		case res := <-par_done:
			log.Println("Par", res)
			tasks_done += 1
			if tasks_done == countOfParsingTasks {
				par_done = nil
				agg_done = make(chan TaskResult)
				tasks_done = 0
				deadline = startTime.Add(WHOLE_TIME)
				for i, task := range agg_tasks {
					log.Println("Send task number ", i, task)
					go handleTask(task, agg_done, deadline)
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
