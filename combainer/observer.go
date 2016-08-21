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
	"github.com/gorilla/mux"
	"github.com/kr/pretty"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/configs"
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

var GlobalObserver = Observer{
	clients: make(map[string]*Client),
}

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
	for config, client := range GlobalObserver.GetClients() {
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

func ParsingConfigs(s ServerContext, w http.ResponseWriter, r *http.Request) {
	list, _ := s.GetRepository().ListParsingConfigs()
	json.NewEncoder(w).Encode(&list)
}

func ReadParsingConfig(s ServerContext, w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	repo := s.GetRepository()
	combainerCfg := repo.GetCombainerConfig()
	var parsingCfg configs.ParsingConfig
	cfg, err := repo.GetParsingConfig(name)
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

func Tasks(s ServerContext, w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	cl, err := NewClient(s.GetContext(), s.GetRepository())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

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

func Launch(s ServerContext, w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	name := mux.Vars(r)["name"]

	logger := log.New()
	logger.Level = log.DebugLevel
	logger.Formatter = s.GetContext().Logger.Formatter
	logger.Out = w

	ctx := &Context{
		Logger: logger,
		Cache:  s.GetContext().Cache,
		Hosts:  s.GetContext().Hosts,
	}

	cl, err := NewClient(ctx, s.GetRepository())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ID := GenerateSessionId()
	err = cl.Dispatch(name, ID, false)
	fmt.Fprintf(w, "%s\n", ID)
	w.(http.Flusher).Flush()
	if err != nil {
		fmt.Fprintf(w, "FAILED: %v\n", err)
		return
	}
	fmt.Fprint(w, "DONE")
}

type ServerContext interface {
	GetContext() *Context
	GetRepository() configs.Repository
}

func attachServer(s ServerContext,
	wrapped func(s ServerContext, w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		wrapped(s, w, r)
	}
}

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
