package combainer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"sync"
	"syscall"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/configs"

	"github.com/go-martini/martini"
)

type StatInfo struct {
	ParsingSuccess   int
	ParsingFailed    int
	ParsingTotal     int
	AggregateSuccess int
	AggregateFailed  int
	AggregateTotal   int
	Heartbeated      int64
}
type OpenFiles struct {
	Open  uint64
	Limit syscall.Rlimit
}

type info struct {
	GoRoutines int
	Files      OpenFiles
	Clients    map[string]*StatInfo
}

var _observer = Observer{clients: make(map[string]*Client)}

type Observer struct {
	sync.RWMutex
	clients map[string]*Client // map active clients to configs
}

func (o *Observer) RegisterClient(cl *Client, config string) {
	o.RWMutex.Lock()
	defer o.RWMutex.Unlock()
	o.clients[config] = cl
}

func (o *Observer) UnregisterClient(config string) {
	o.RWMutex.Lock()
	defer o.RWMutex.Unlock()
	delete(o.clients, config)
}

func (o *Observer) GetClients() (clients map[string]*Client) {
	o.RLock()
	defer o.RUnlock()
	clients = make(map[string]*Client)
	clients = o.clients
	return
}

func (o *Observer) GetClient(config string) *Client {
	o.RLock()
	defer o.RUnlock()
	return o.clients[config]
}

func Dashboard(w http.ResponseWriter, r *http.Request) {
	get_number_openfiles := func() uint64 {
		files, _ := ioutil.ReadDir("/proc/self/fd")
		return uint64(len(files))
	}

	w.Header().Set("Content-Type", "application/json")
	stats := make(map[string]*StatInfo)
	for config, client := range _observer.GetClients() {
		stats[config] = client.GetStats()
	}

	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		fmt.Fprintf(w, "{\"error\": \"unable to dump json %s\"", err)
		return
	}

	if err := json.NewEncoder(w).Encode(info{
		GoRoutines: runtime.NumGoroutine(),
		Files: OpenFiles{
			get_number_openfiles(),
			limit,
		},
		Clients: stats,
	}); err != nil {
		fmt.Fprintf(w, "{\"error\": \"unable to dump json %s\"", err)
		return
	}
}

func ParsingConfigs(repo configs.Repository, w http.ResponseWriter, r *http.Request) {
	list, _ := repo.ListParsingConfigs()
	json.NewEncoder(w).Encode(&list)
}

func ReadParsingConfig(repo configs.Repository, params martini.Params, w http.ResponseWriter) {
	name := params["name"]
	combainerCfg := repo.GetCombainerConfig()
	var parsingCfg configs.ParsingConfig
	r, err := repo.GetParsingConfig(name)
	if err != nil {
		fmt.Fprintf(w, "%s", err)
		return
	}

	err = r.Decode(&parsingCfg)
	if err != nil {
		fmt.Fprintf(w, "%s", err)
		return
	}

	parsingCfg.UpdateByCombainerConfig(&combainerCfg)
	aggregationConfigs, err := GetAggregationConfigs(repo, &parsingCfg)
	if err != nil {
		LogErr("Unable to read aggregation configs: %s", err)
		return
	}

	data, err := common.Encode(&parsingCfg)
	if err != nil {
		fmt.Fprintf(w, "%s", err)
		return
	}

	fmt.Fprintf(w, "============ %s ============\n", name)
	fmt.Fprintf(w, "%s", data)
	for aggname, v := range *aggregationConfigs {
		fmt.Fprintf(w, "============ %s ============\n", aggname)
		d, err := common.Encode(&v)
		if err != nil {
			fmt.Fprintf(w, "%s", err)
			return
		}
		fmt.Fprintf(w, "%s\n", d)
	}
}

func Tasks(repo configs.Repository, context *Context, params martini.Params, w http.ResponseWriter) {
	name := params["name"]
	combainerConfig := repo.GetCombainerConfig()
	cl, err := NewClient(context, combainerConfig, repo)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sp, err := cl.UpdateSessionParams(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = json.NewEncoder(w).Encode(sp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func StartObserver(endpoint string, services ...interface{}) {
	m := martini.Classic()
	for _, service := range services {
		m.Map(service)
	}
	m.Group("/parsing", func(r martini.Router) {
		r.Get("/", ParsingConfigs)
		r.Get("", ParsingConfigs)
		r.Get("/:name", ReadParsingConfig)
	})
	m.Get("/tasks/:name", Tasks)

	m.Get("/", Dashboard)
	http.ListenAndServe(endpoint, m)
}
