package combainer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"sync"
	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/go-martini/martini"
//	"github.com/hashicorp/memberlist"
	"github.com/kr/pretty"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/configs"
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
	getNumberOfOpenfiles := func() uint64 {
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
			getNumberOfOpenfiles(),
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
		log.Errorf("Unable to read aggregation configs: %s", err)
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
	cl, err := NewClient(context, repo)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sp, err := cl.UpdateSessionParams(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for n, task := range sp.PTasks {
		fmt.Fprintf(w, "============ (%d/%d) ============\n", n+1, len(sp.PTasks))
		fmt.Fprintf(w, "%# v\n", pretty.Formatter(task))
	}
}

func Launch(repo configs.Repository, context *Context, params martini.Params, w http.ResponseWriter) {
	name := params["name"]
	cl, err := NewClient(context, repo)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ID := GenerateSessionId()
	err = cl.Dispatch(name, ID, false)
	fmt.Fprintf(w, "%s\n", ID)
	w.(http.Flusher).Flush()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprint(w, "DONE")
}

/*
func Cluster(cluster *memberlist.Memberlist, w http.ResponseWriter) {
	for _, node := range cluster.Members() {
		fmt.Fprintf(w, "%s %s\n", node.Name, node.Addr)
	}
}
*/

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
	m.Get("/launch/:name", Launch)
//	m.Get("/cluster", Cluster)

	m.Get("/", Dashboard)
	http.ListenAndServe(endpoint, m)
}
