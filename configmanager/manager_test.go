package configmanager

import (
	"testing"
)

func TestParsingRead(t *testing.T) {
	date, err := GetParsingCfg("photo_proxy")
	if err != nil {
		t.Fatal(err)
	}
	if len(date) == 0 {
		t.Fatal("Zero data")

	}
	t.Log(date)

	_, err = GetParsingCfg("missing_filename")
	if err == nil {
		t.Fatal(err)
	}
}

func TestAggregateRead(t *testing.T) {
	_, err := GetAggregateCfg("http_ok")
	if err != nil {
		t.Fatal(err)
	}
	_, err = GetAggregateCfg("missing_filename")
	if err == nil {
		t.Fatal(err)
	}
}
