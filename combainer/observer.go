package combainer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
)

type StatInfo struct {
	Success     int
	Failed      int
	Total       int
	Heartbeated int64
}

type info struct {
	GoRoutines int
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
	w.Header().Set("Content-Type", "application/json")
	stats := make(map[string]*StatInfo)
	for config, client := range _observer.GetClients() {
		stats[config] = client.GetStats()
	}

	err := json.NewEncoder(w).Encode(info{
		GoRoutines: runtime.NumGoroutine(),
		Clients:    stats,
	})

	if err != nil {
		fmt.Fprintf(w, "{\"error\": \"unable to dump json %s\"", err)
	}
}

func StartObserver(endpoint string) {
	http.HandleFunc("/", Dashboard)
	http.ListenAndServe(endpoint, nil)
}
