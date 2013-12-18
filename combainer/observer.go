package combainer

import (
	"bytes"
	"runtime"
	"sync"
	"text/template"

	"github.com/hoisie/web"
)

type info struct {
	GoRoutines int
	Clients    map[string]*StatInfo
}

const _DASHBOARD = `
{"GoRoutines": {{.GoRoutines}}, 
 "Clients": {{range $index, $element := .Clients}}
 {{$index}},
 {{$element}}
{{end}} 
}`

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

func Dashboard(ctx *web.Context) string {
	ctx.SetHeader("X-Powered-By", "web.go", true)
	ctx.ContentType("application/json")
	var output bytes.Buffer
	stats := make(map[string]*StatInfo)
	for config, client := range _observer.GetClients() {
		stats[config] = client.GetStats()
	}
	if err := dashboard.Execute(&output, info{runtime.NumGoroutine(), stats}); err != nil {
		return ERROR
	} else {
		return output.String()
	}
}

func getClinet(config string) string {
	if cl := _observer.GetClient(config); cl != nil {
		return config
	} else {
		return "ERROR"
	}
}

func StartObserver(endpoint string) {

	web.Get("/REST/", Dashboard)
	web.Get("/REST/(.+)", getClinet)
	web.Run(endpoint)
}
