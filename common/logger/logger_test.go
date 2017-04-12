package logger

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestLogrusLevelFlag(t *testing.T) {
	var lvl LogrusLevelFlag
	assert.Error(t, lvl.Set("Unknown"))
	assert.Nil(t, lvl.Set("DEBUG"))
	assert.Equal(t, "debug", lvl.String())
}

func TestInitializeLogger(t *testing.T) {
	tmpfile := "/tmp/__tmpLogFile.log"
	InitializeLogger(logrus.DebugLevel, tmpfile)
	if _, err := os.Stat(tmpfile); os.IsNotExist(err) {
		t.Fatalf("Failed to initialize logger file %s", tmpfile)
	}
	defer os.Remove(tmpfile) // clean up

	CocaineLog = LocalLogger()
	CocaineLog.Err("line1")
	CocaineLog.Errf("line2")

	stat, err := os.Stat(tmpfile)
	assert.NoError(t, err)
	assert.True(t, stat.Size() > 10)

	os.Remove(tmpfile) // clean up
	_, err = os.Stat(tmpfile)
	assert.True(t, os.IsNotExist(err), fmt.Sprintf("File %s should not exists", tmpfile))
	err = exec.Command("kill", "-HUP", strconv.Itoa(os.Getpid())).Run()
	assert.NoError(t, err, "Failed to send SIGHUP yourself")
	time.Sleep(20 * time.Millisecond)
	if _, err := os.Stat(tmpfile); os.IsNotExist(err) {
		t.Fatalf("Failed to rotate logger file %s: %s", tmpfile, err)
	}
	Debugf("line3")
	Infof("line4")
	Errf("line5")
	Warnf("line6")

	expected := []struct {
		level string
		msg   string
	}{
		{"DEBUG", "line3"},
		{"INFO", "line4"},
		{"ERROR", "line5"},
		{"WARN", "line6"},
	}
	bytes, err := ioutil.ReadFile(tmpfile)
	assert.NoError(t, err, "Failed to read log file")
	text := strings.TrimRight(string(bytes), "\n")
	checkText := func() {
		for i, line := range strings.Split(text, "\n") {
			assert.Contains(t, line, expected[i].level)
			assert.Contains(t, line, expected[i].msg)
		}
	}
	assert.NotPanics(t, checkText)
}
