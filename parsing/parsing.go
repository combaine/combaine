package parsing

import (
	_ "encoding/json"
	"fmt"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	_ "launchpad.net/goyaml"
)

/*
1. Fetch data
2. Send to parsing
3. Send datagrid application
4. Call aggregators
*/

type Task struct {
	Host     string
	Config   string
	Group    string
	PrevTime int
	CurrTime int
	Id       string
}

func (t *Task) String() string {
	return fmt.Sprintf("%v", t)
}

// {"host": "imagick01g.photo.yandex.ru", "logname": "nginx/access.log", "timetail_url": "/timetail?log=", "timetail_port": 3132, "StartTime": 300}
func Parsing(task Task) error {
	log := cocaine.NewLogger()
	log.Info("Start ", task)
	defer log.Close()

	log.Debug("Create configuration manager")
	cfgManager := cocaine.NewService("cfgmanager")
	defer cfgManager.Close()

	log.Debug("Receive configuration file")
	res := <-cfgManager.Call("enqueue", "parsing", task.Config)
	if res.Err() != nil {
		return res.Err()
	}

	// var m combainerConfig
	// err = goyaml.Unmarshal(data, &m)
	// if err != nil {
	// 	return nil, err
	// }

	// fetcher := cocaine.NewService("timetail")

	// js, _ := json.Marshal(struct {
	// 	Host    string `json:"host"`
	// 	Logname string `json:"logname"`
	// 	URL     string `json:"timetail_url"`
	// 	Port    int    `json:"timetail_port"`
	// 	Tz      int    `json:"StartTime"`
	// }{
	// 	"imagick01g.photo.yandex.ru",
	// 	"nginx/access.log",
	// 	"/timetail?log=",
	// 	3132,
	// 	300,
	// })
	// fmt.Println(string(js))
	// res := <-fetcher.Call("enqueue", "get", js)
	// var t []byte
	// fmt.Println(res.Err())
	// res.Extract(&t)
	// fmt.Println(string(t))
	return nil
}
