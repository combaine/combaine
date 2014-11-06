package combainer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"sync"
	"syscall"
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

func StartObserver(endpoint string) {
	http.HandleFunc("/", Dashboard)
	http.ListenAndServe(endpoint, nil)
}
