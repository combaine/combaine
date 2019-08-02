package worker

import (
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/combaine/combaine/senders"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	grpc "google.golang.org/grpc"
)

var sLock sync.Mutex
var services = map[string]*grpc.ClientConn{}
var aggregatorConnection *grpc.ClientConn
var aggregatorService = "aggregator.py"
var appsDir = "/usr/lib/combaine/apps/"
var logDir = "/var/log/combaine/"
var envPrefix = "COMBAINE_WORKER_SERVICE_"

// concurrent write access only on initialization step
func register(name string, conn *grpc.ClientConn) {
	sLock.Lock()
	services[name] = conn
	sLock.Unlock()
}

// GetSenderClient return grpc client to locally spawned senders
func GetSenderClient(sType string) (senders.SenderClient, error) {
	if conn, ok := services[sType]; ok {
		return senders.NewSenderClient(conn), nil
	}
	return nil, errors.New("Unknown sender type: " + sType)
}

func spawnService(name string, port int, stopCh chan bool) (*grpc.ClientConn, error) {
	options := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(1024*1024*128),
			grpc.MaxCallRecvMsgSize(1024*1024*128),
		),
	}
	envServicePrefix := envPrefix + strings.ToUpper(name)
	logoutput, found := os.LookupEnv(envServicePrefix + "_LOGOUTPUT")
	if !found {
		logoutput = logDir + name + ".log"
	}
	loglevel, found := os.LookupEnv(envServicePrefix + "_LOGLEVEL")
	if !found {
		loglevel = "info"
	}
	var targetPort = ":" + strconv.Itoa(port)
	var endpoint = "[::]" + targetPort
	go func() {
		logrus.Infof("Spawn %s", name)
		for {
			select {
			case <-stopCh:
				logrus.Infof("spawnService interrupted")
				return
			default:
				cmd := exec.Command(
					appsDir+name,
					"-endpoint", endpoint,
					"-logoutput", logoutput,
					"-loglevel", loglevel,
				)
				cmd.SysProcAttr = &syscall.SysProcAttr{
					Pdeathsig: syscall.SIGTERM,
				}
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				err := cmd.Run()
				if err != nil {
					logrus.Errorf("Run failed for %s: %v", name, err)
				}
				logrus.Infof("Service %s stopped", name)
				time.Sleep(time.Second * 3)
				logrus.Infof("Respawn %s", name)
			}
		}
	}()

	var attempts = 20
	for i := 0; i <= attempts; i++ {
		cc, err := grpc.Dial(targetPort, options...)
		if err != nil {
			logrus.Warnf("Failed to dial %s: %v", name, err)
			time.Sleep(time.Second * 5)
		} else {
			return cc, nil
		}
	}
	return nil, errors.Errorf("Failed to dial %s (%d times)", name, attempts)
}

// SpawnServices spawn subprocesses with services
func SpawnServices(stopCh chan bool) error {

	os.Stdout.WriteString("ENVIRON\n")
	for _, s := range os.Environ() {
		os.Stdout.WriteString(s + "\n")
	}

	port := 10000
	acc, err := spawnService("aggregator/"+aggregatorService, port, stopCh)
	if err != nil {
		return err
	}
	aggregatorConnection = acc

	files, err := ioutil.ReadDir(appsDir)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		port++
		wg.Add(1)
		go func(n string, p int) {
			sc, err := spawnService(n, p, stopCh)
			if err != nil {
				errCh <- err
				return
			}
			register(n, sc)
			wg.Done()
		}(f.Name(), port)
	}
	go func() {
		wg.Wait()
		close(errCh)
	}()
	var result error
	for err := range errCh {
		if err != nil {
			result = multierror.Append(result, err)
		}
	}

	return result
}
