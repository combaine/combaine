package parsing

import (
	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/noxiouz/Combaine/common"
)

// Wrapper around cfgmanager. TBD: move to common
type cfgWrapper struct {
	cfgManager *cocaine.Service
	log        *cocaine.Logger
}

func (m *cfgWrapper) GetParsingConfig(name string) (cfg common.ParsingConfig, err error) {
	res := <-m.cfgManager.Call("enqueue", "parsing", name)
	if err = res.Err(); err != nil {
		return
	}
	var rawCfg []byte
	if err = res.Extract(&rawCfg); err != nil {
		return
	}
	err = common.Encode(rawCfg, &cfg)
	return
}

func (m *cfgWrapper) GetAggregateConfig(name string) (cfg common.AggConfig, err error) {
	res := <-m.cfgManager.Call("enqueue", "aggregate", name)
	if err = res.Err(); err != nil {
		log.Err(err)
		return
	}
	var rawCfg []byte
	if err = res.Extract(&rawCfg); err != nil {
		log.Err(err)
		return
	}

	err = common.Encode(rawCfg, &cfg)
	return
}

func (m *cfgWrapper) GetCommon() (combainerCfg common.CombainerConfig, err error) {
	res := <-m.cfgManager.Call("enqueue", "common", "")
	if err = res.Err(); err != nil {
		return
	}
	var rawCfg []byte
	if err = res.Extract(&rawCfg); err != nil {
		return
	}
	err = common.Encode(rawCfg, &combainerCfg)
	return
}

func (m *cfgWrapper) Close() {
	m.cfgManager.Close()
}
