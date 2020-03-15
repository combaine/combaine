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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestInitializeLogger(t *testing.T) {
	tmpfile := "/tmp/__tmpLogFile.log"
	*LogLevel = "debug"
	*LogOutput = tmpfile
	InitializeLogger()
	if _, err := os.Stat(tmpfile); os.IsNotExist(err) {
		t.Fatalf("Failed to initialize logger file %s", tmpfile)
	}
	defer os.Remove(tmpfile) // clean up

	logrus.Error("line1")
	logrus.Errorf("line2")

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
	logrus.Debugf("line3")
	logrus.Infof("line4")
	logrus.Errorf("line5")
	logrus.Warnf("line6")

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
