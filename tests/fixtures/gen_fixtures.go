package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/noxiouz/Combaine/common/configs"
	"github.com/noxiouz/Combaine/common/tasks"
	"github.com/ugorji/go/codec"
)

// type ParsingTask struct {
// 	CommonTask
// 	// Hostname of target
// 	Host string
// 	// Name of handled parsing config
// 	ParsingConfigName string
// 	// Content of the current parsing config
// 	ParsingConfig configs.ParsingConfig
// 	// Content of aggreagtion configs
// 	// related to the current parsing config
// 	AggregationConfigs map[string]configs.AggregationConfig
// }

// type ParsingConfig struct {
// 	// List of host groups
// 	// MUST consist of 1 value now.
// 	Groups []string `yaml:"groups"`
// 	// List of names of Aggregation configs
// 	AggConfigs []string `yaml:"agg_configs"`
// 	// Name of parsing function, which is used to parse data
// 	// Set it `NullParser` or leave empty
// 	// to skip the parsing of data.
// 	Parser string `yaml:"parser"`
// 	// Overrides the same section in combainer.yaml
// 	DataFetcher PluginConfig `yaml:"DataFetcher"`
// 	// Overrides name of host group
// 	Metahost string `yaml:"metahost"`
// 	// Set True to skip putting data into DataBase
// 	Raw         bool `yaml:"raw"`
// 	MainSection `yaml:"Combainer"`
// }
var (
	mh codec.MsgpackHandle
	b  []byte
	h  = &mh // or mh to use msgpack
)

func main() {
	const (
		FIXTURE_PATH = "tests/fixtures"

		HOST              = "TESTHOST"
		PARSINGCONFIGNAME = "PARSINGCONFIGNAME"
		PARSER            = "PARSER"
		METAHOST          = "METAHOST"
	)

	pwd, _ := os.Getwd()
	fixture_path := path.Join(pwd, FIXTURE_PATH)

	var (
		Groups     = []string{"A", "B"}
		AggConfigs = []string{"A", "B"}
	)

	parsing_config := configs.ParsingConfig{
		Groups:     Groups,
		AggConfigs: AggConfigs,
		Parser:     PARSER,
		Metahost:   METAHOST,
	}

	parsing_task := tasks.ParsingTask{
		Host:              HOST,
		ParsingConfigName: PARSINGCONFIGNAME,
		ParsingConfig:     parsing_config,
	}

	pc_js, err := json.Marshal(parsing_task)
	if err != nil {
		log.Fatal(err)
	}

	var pc_msg []byte
	if err := codec.NewEncoderBytes(&pc_msg, h).Encode(parsing_task); err != nil {
		log.Fatal(err)
	}

	log.Println("Generating parsing_config json fixture...")
	if err := ioutil.WriteFile(path.Join(fixture_path, "fixture_json_parsing_task"), pc_js, 0777); err != nil {
		log.Fatal(err)
	}
	log.Println("Generating parsing_config msgpack fixture...")
	if err := ioutil.WriteFile(path.Join(fixture_path, "fixture_msgpack_parsing_task"), pc_msg, 0777); err != nil {
		log.Fatal(err)
	}
}
