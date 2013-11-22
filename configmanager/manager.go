package configmanager

import (
	"fmt"
	"io/ioutil"
)

const PARSING_PATH = "/etc/combaine/parsing/"
const AGGREGATE_PATH = "/etc/combaine/aggregate/"
const COMBAINE_PATH = "/etc/combaine/"

func GetParsingCfg(name string) ([]byte, error) {
	filename := fmt.Sprintf("%s%s", PARSING_PATH, name)
	return getConfig(filename)
}

func GetAggregateCfg(name string) ([]byte, error) {
	filename := fmt.Sprintf("%s%s", AGGREGATE_PATH, name)
	return getConfig(filename)
}

func GetCommonCfg() (res []byte, err error) {
	res, err = getConfig(fmt.Sprintf("%s%s", COMBAINE_PATH, "combaine"))
	return
}

func getConfig(path string) (res []byte, err error) {
	res, err = ioutil.ReadFile(fmt.Sprintf("%s.%s", path, "yaml"))
	if err == nil {
		return
	}

	res, err = ioutil.ReadFile(fmt.Sprintf("%s.%s", path, "json"))
	if err == nil {
		return
	}
	res, err = ioutil.ReadFile(fmt.Sprintf("%s", path))
	return
}
