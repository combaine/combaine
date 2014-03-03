package combainer

import (
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"text/template"
)

type clientStats struct {
	sync.RWMutex
	success int
	failed  int
}

type StatInfo struct {
	Success int
	Failed  int
	Total   int
}

type info struct {
	GoRoutines int
	Clients    map[string]*StatInfo
}

const _DASHBOARD = `
{"GoRoutines": {{.GoRoutines}}, 
 "Clients": {{range $index, $element := .Clients}}
 {{$index}},
 {{$element}}
{{end}}}`

const ERROR = `{"error": "unknown error"}`

var _observer = Observer{clients: make(map[string]*Client)}

var dashboard *template.Template = template.Must(template.New("dashboard").Parse(_DASHBOARD))

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
	if err := dashboard.Execute(w, info{runtime.NumGoroutine(), stats}); err != nil {
		w.Write([]byte(fmt.Sprintf("{ \"Error\": %s }", err.Error())))
	}
}

func StartObserver(endpoint string) {
	http.HandleFunc("/", Dashboard)
	http.ListenAndServe(endpoint, nil)
}
