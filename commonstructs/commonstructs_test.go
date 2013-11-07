package commonstructs

import (
	"fmt"
	"testing"

	"github.com/noxiouz/Combaine/configmanager"
	"launchpad.net/goyaml"
)

func TestParsing(t *testing.T) {
	var m ParsingConfig
	data, err := configmanager.GetParsingCfg("photo_proxy.json")
	if err != nil {
		t.Fatal(err)
	}
	err = goyaml.Unmarshal(data, &m)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(m.Groups, m.AggConfigs, m.Parser)
}

func TestAggregate(t *testing.T) {
	var m AggConfig
	data, err := configmanager.GetAggregateCfg("http_ok.yaml")
	if err != nil {
		t.Fatal(err)
	}
	err = goyaml.Unmarshal(data, &m)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(len(m.Data))
	fmt.Println(len(m.Senders))

	t.Log(m)
}
