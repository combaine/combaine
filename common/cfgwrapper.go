package common

import (
	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/noxiouz/Combaine/common/configs"
)

// Wrapper around cfgmanager. TBD: move to common
type CfgWrapper struct {
	cfgManager *cocaine.Service
	log        *cocaine.Logger
}

func NewCfgWrapper(s *cocaine.Service, l *cocaine.Logger) *CfgWrapper {
	return &CfgWrapper{s, l}
}

func (m *CfgWrapper) GetParsingConfig(name string) (cfg configs.ParsingConfig, err error) {
	res := <-m.cfgManager.Call("enqueue", "parsing", name)
	if err = res.Err(); err != nil {
		return
	}
	var rawCfg []byte
	if err = res.Extract(&rawCfg); err != nil {
		return
	}
	err = Encode(rawCfg, &cfg)
	return
}

func (m *CfgWrapper) GetAggregateConfig(name string) (cfg configs.AggregationConfig, err error) {
	res := <-m.cfgManager.Call("enqueue", "aggregate", name)
	if err = res.Err(); err != nil {
		m.log.Err(err)
		return
	}
	var rawCfg []byte
	if err = res.Extract(&rawCfg); err != nil {
		m.log.Err(err)
		return
	}

	err = Encode(rawCfg, &cfg)
	return
}

func (m *CfgWrapper) GetCommon() (combainerCfg configs.CombainerConfig, err error) {
	res := <-m.cfgManager.Call("enqueue", "common", "")
	if err = res.Err(); err != nil {
		return
	}
	var rawCfg []byte
	if err = res.Extract(&rawCfg); err != nil {
		return
	}
	err = Encode(rawCfg, &combainerCfg)
	return
}

func (m *CfgWrapper) Close() {
	m.cfgManager.Close()
}
