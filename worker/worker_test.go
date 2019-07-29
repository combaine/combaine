package worker

import (
	fmt "fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/combaine/combaine/fetchers"
	"github.com/combaine/combaine/repository"
	"github.com/stretchr/testify/assert"
)

const repoPath = "../testdata/configs"

var (
	ts *httptest.Server
)

func TestFetchersRegistered(t *testing.T) {
	c := repository.PluginConfig{
		"port":          1,
		"timetail_port": 1,
	}
	_, err := fetchers.NewFetcher("tcpsocket", c)
	assert.NoError(t, err)
	_, err = fetchers.NewFetcher("http", c)
	assert.NoError(t, err)
	_, err = fetchers.NewFetcher("timetail", c)
	assert.NoError(t, err)

}

func TestMain(m *testing.M) {
	if err := repository.Init(repoPath); err != nil {
		log.Fatal(err)
	}
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/timetail":
			pattern := r.URL.Query().Get("pattern")
			if pattern == "" {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintln(w, "Query parameter pattern not specified")
				return
			}
			fileName := fmt.Sprintf("testdata/payload/%s.txt", pattern)
			resp, err := ioutil.ReadFile(fileName)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "Failed to read file %s, %s", fileName, err)
				return
			}
			w.Header().Set("Content-Type", "text/plain")
			w.Write(resp)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "Not Found")
		}
	}))
	os.Exit(m.Run())
}
