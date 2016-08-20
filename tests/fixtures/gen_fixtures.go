package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/ugorji/go/codec"

	"github.com/combaine/combaine/common/configs"
	"github.com/combaine/combaine/common/tasks"
)

var (
	mh codec.MsgpackHandle
	b  []byte
	h  = &mh // or mh to use msgpack
)

func main() {
	const (
		FIXTURE_PATH = "tests/fixtures"

		HOST                  = "TESTHOST"
		PARSINGCONFIGNAME     = "PARSINGCONFIGNAME"
		AGGREGATIONCONFIGNAME = "AGGREGATIONCONFIGNAME"
		PARSER                = "PARSER"
		METAHOST              = "METAHOST"
	)

	cm_task := tasks.CommonTask{
		Id:       "UNIQID",
		PrevTime: 101,
		CurrTime: 120,
	}

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

	aggregation_config := configs.AggregationConfig{
		Senders: map[string]configs.PluginConfig{
			"A": {
				"type": "juggler",
				"Args": 1,
			},
		},
		Data: map[string]configs.PluginConfig{
			"20x": {
				"type":  "summa",
				"Query": "SELECT COUNT FROM *",
			},
		},
	}

	hostsAndDc := map[string][]string{
		"DC1": {"Host1", "Host2"},
		"DC2": {"Host3", "Host4"},
	}

	parsing_task := tasks.ParsingTask{
		CommonTask:        cm_task,
		Host:              HOST,
		ParsingConfigName: PARSINGCONFIGNAME,
		ParsingConfig:     parsing_config,
	}

	aggregation_task := tasks.AggregationTask{
		CommonTask:        cm_task,
		Config:            AGGREGATIONCONFIGNAME,
		ParsingConfigName: PARSINGCONFIGNAME,
		ParsingConfig:     parsing_config,
		AggregationConfig: aggregation_config,
		Hosts:             hostsAndDc,
	}

	gen := func(input interface{}) (js, msg []byte) {
		if err := codec.NewEncoderBytes(&msg, h).Encode(input); err != nil {
			log.Fatal(err)
		}

		js, err := json.Marshal(input)
		if err != nil {
			log.Fatal(err)
		}
		return
	}
	pc_js, pc_msg := gen(parsing_task)
	agg_js, agg_msg := gen(aggregation_task)

	log.Println("Generating parsing_json fixture...")
	if err := ioutil.WriteFile(path.Join(fixture_path, "fixture_json_parsing_task"), pc_js, 0666); err != nil {
		log.Fatal(err)
	}
	log.Println("Generating parsing_config msgpack fixture...")
	if err := ioutil.WriteFile(path.Join(fixture_path, "fixture_msgpack_parsing_task"), pc_msg, 0666); err != nil {
		log.Fatal(err)
	}

	log.Println("Generating aggregation_task json fixture...")
	if err := ioutil.WriteFile(path.Join(fixture_path, "fixture_json_aggregation_task"), agg_js, 0666); err != nil {
		log.Fatal(err)
	}
	log.Println("Generating aggregation_task msgpack fixture...")
	if err := ioutil.WriteFile(path.Join(fixture_path, "fixture_msgpack_aggregation_task"), agg_msg, 0666); err != nil {
		log.Fatal(err)
	}
}
