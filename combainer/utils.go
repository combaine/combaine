package combainer

import (
	"fmt"
	"io/ioutil"
	"launchpad.net/goyaml"
	"log"
	"net/http"
	"strings"
)

const (
	CONFIGS_PARSING_PATH = "/etc/combaine/parsing/"
	COMBAINER_PATH       = "/etc/combaine/combaine.yaml"
)

type ParsingConfig struct {
	Groups     []string "groups"
	AggConfigs []string "agg_configs"
	Metahost   string   "metahost"
}

// Fetch hosts by groupname from HTTP
func GetHosts(handle string, groupname string) (hosts []string, err error) {
	url := fmt.Sprintf("%s%s", handle, groupname)
	if strings.HasPrefix(url, "http:") { // Over HTTP
		//log.Println("Fetch hosts: ", url)
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		//log.Println("Fetch hosts, status code", resp.StatusCode)
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		} else {
			s := strings.TrimSuffix(string(body), "\n")
			return strings.Split(s, "\n"), nil
		}
	} else { // File
		data, err := ioutil.ReadFile(url)
		if err != nil {
			log.Println(err)
			return nil, err
		} else {
			s := strings.TrimSuffix(string(data), "\n")
			return strings.Split(s, "\n"), nil
		}
	}
}

// Return listing of configuration files
func getConfigs(path string) []string {
	var s []string
	files, _ := ioutil.ReadDir(path)
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".json") || strings.HasSuffix(f.Name(), ".yaml") {
			s = append(s, f.Name())
		}
	}
	return s
}

func getParsings() []string {
	return getConfigs(CONFIGS_PARSING_PATH)
}

// Parse config
func loadConfig(name string) (*ParsingConfig, error) {
	path := fmt.Sprintf("%s%s", CONFIGS_PARSING_PATH, name)
	log.Println("Read ", path)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// Parse combaine.yaml
	var res ParsingConfig
	err = goyaml.Unmarshal(data, &res)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return &res, nil
}
