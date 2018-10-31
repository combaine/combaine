package combainer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"sync"
	"syscall"

	"github.com/gorilla/mux"
	"github.com/kr/pretty"
	"github.com/sirupsen/logrus"

	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/utils"
)

// StatInfo contains stats about main operations (aggregating and parsing)
type StatInfo struct {
	ParsingSuccess   int64
	ParsingFailed    int64
	ParsingTotal     int64
	AggregateSuccess int64
	AggregateFailed  int64
	AggregateTotal   int64
	Heartbeated      int64
}

// OpenFiles contains info abound fd usage
type OpenFiles struct {
	Open  uint64
	Limit syscall.Rlimit
}

type info struct {
	GoRoutines int
	Files      OpenFiles
	Clients    map[string]*StatInfo
}

// GlobalObserver is storage for client registations
var GlobalObserver = Observer{
	clients: make(map[string]*Client),
}

// Observer object with registered clients
type Observer struct {
	sync.RWMutex
	clients map[string]*Client // map active clients to configs
}

// RegisterClient register client in Observer
// ReRegister client is UnregisterClient for previously
// registered client, but all stats are copied
func (o *Observer) RegisterClient(cl *Client, config string) {
	o.RWMutex.Lock()
	if oldCl, ok := o.clients[config]; ok {
		oldCl.CopyStats(&cl.clientStats)
	}
	o.clients[config] = cl
	o.RWMutex.Unlock()
}

// UnregisterClient unregister client in Observer
// Deregister only a yourself by checking id
func (o *Observer) UnregisterClient(id uint64, config string) {
	o.RWMutex.Lock()
	if cl, ok := o.clients[config]; ok && cl.ID == id {
		delete(o.clients, config)
	}
	o.RWMutex.Unlock()
}

// GetClientsStats return map with client stats
func (o *Observer) GetClientsStats() map[string]*StatInfo {
	stats := make(map[string]*StatInfo)
	o.RLock()
	for config, client := range o.clients {
		stats[config] = client.GetStats()
	}
	o.RUnlock()
	return stats
}

// Dashboard handle http request abount internal statistics
func Dashboard(w http.ResponseWriter, r *http.Request) {
	getNumberOfOpenfiles := func() uint64 {
		files, _ := ioutil.ReadDir("/proc/self/fd")
		return uint64(len(files))
	}

	w.Header().Set("Content-Type", "application/json")
	stats := GlobalObserver.GetClientsStats()

	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		fmt.Fprintf(w, `{"error": "unable to dump json %s"`, err)
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
		fmt.Fprintf(w, `{"error": "unable to dump json %s"`, err)
		return
	}
}

// ParsingConfigs list parsing configs names
func ParsingConfigs(s ServerContext, w http.ResponseWriter, r *http.Request) {
	list, _ := repository.ListParsingConfigs()
	json.NewEncoder(w).Encode(&list)
}

// ReadParsingConfig return parsing config content
// before return UpdateByCombainerConfig update config
func ReadParsingConfig(s ServerContext, w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	combainerCfg := repository.GetCombainerConfig()
	var parsingCfg repository.ParsingConfig
	cfg, err := repository.GetParsingConfig(name)
	if err != nil {
		fmt.Fprintf(w, "%s", err)
		return
	}

	err = cfg.Decode(&parsingCfg)
	if err != nil {
		fmt.Fprintf(w, "%s", err)
		return
	}

	parsingCfg.UpdateByCombainerConfig(&combainerCfg)
	aggregationConfigs, err := repository.GetAggregationConfigs(&parsingCfg, name)
	if err != nil {
		logrus.Errorf("Unable to read aggregation configs: %s", err)
		return
	}

	data, err := parsingCfg.Encode()
	if err != nil {
		fmt.Fprintf(w, "%s", err)
		return
	}

	fmt.Fprintf(w, "============ %s ============\n", name)
	fmt.Fprintf(w, "%s", data)
	for aggname, v := range *aggregationConfigs {
		fmt.Fprintf(w, "============ %s ============\n", aggname)
		d, err := v.Encode()
		if err != nil {
			fmt.Fprintf(w, "%s", err)
			return
		}
		fmt.Fprintf(w, "%s\n", d)
	}
}

// Tasks return information about parsing tasks
// that should be performed by config
func Tasks(s ServerContext, w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	cl, err := NewClient()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer cl.Close()

	sp, err := cl.updateSessionParams(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for n, task := range sp.PTasks {
		fmt.Fprintf(w, "============ (%d/%d) ============\n", n+1, len(sp.PTasks))
		fmt.Fprintf(w, "%# v\n", pretty.Formatter(task))
	}
}

// Launch run full iteration for config
func Launch(s ServerContext, w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	name := mux.Vars(r)["name"]

	cl, err := NewClient(withDebugLaunchLoggerOpt(w))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer cl.Close()
	ID := "launch-" + utils.GenerateSessionID()
	err = cl.Dispatch(0, name, ID, false)
	fmt.Fprintf(w, "%s\n", ID)
	w.(http.Flusher).Flush()
	if err != nil {
		fmt.Fprintf(w, "FAILED: %v\n", err)
		return
	}
	fmt.Fprint(w, "DONE")
}

func withDebugLaunchLoggerOpt(w http.ResponseWriter) func(*Client) error {
	return func(c *Client) error {
		logger := logrus.New()
		logger.Level = logrus.DebugLevel
		logger.Formatter = &logrus.TextFormatter{
			ForceColors:    true,
			DisableSorting: true,
		}
		logger.Out = w
		c.log = logger
		return nil
	}
}

// ServerContext contains server context with repository
type ServerContext interface {
	GetHosts() []string
}

func attachServer(s ServerContext,
	wrapped func(s ServerContext, w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		wrapped(s, w, r)
	}
}

// GetRouter return mux root router
func GetRouter(context ServerContext) http.Handler {
	root := mux.NewRouter()
	root.StrictSlash(true)

	parsingRouter := root.PathPrefix("/parsing/").Subrouter()
	parsingRouter.StrictSlash(true)
	parsingRouter.HandleFunc("/", attachServer(context, ParsingConfigs)).Methods("GET")
	parsingRouter.HandleFunc("/{name}", attachServer(context, ReadParsingConfig)).Methods("GET")

	root.HandleFunc("/tasks/{name}", attachServer(context, Tasks)).Methods("GET")
	root.HandleFunc("/launch/{name}", attachServer(context, Launch)).Methods("GET")
	root.HandleFunc("/", Dashboard).Methods("GET")

	return root
}
