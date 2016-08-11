package parsing

import (
	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/Combaine/Combaine/common"
)

type Parser interface {
	Parse(tid string, parsername string, data []byte) ([]byte, error)
}

type parser struct {
	app *cocaine.Service
}

func (p *parser) Parse(tid string, parsername string, data []byte) (z []byte, err error) {
	taskToParser, err := common.Pack([]interface{}{tid, parsername, data})
	if err != nil {
		return
	}

	res := <-p.app.Call("enqueue", "parse", taskToParser)
	if err = res.Err(); err != nil {
		return
	}

	if err = res.Extract(&z); err != nil {
		return
	}
	return
}

func GetParser() (p Parser, err error) {
	app, err := cacher.Get(common.PARSINGAPP)
	if err != nil {
		return
	}
	p = &parser{app: app}
	return
}
