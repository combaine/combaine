package worker

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/combaine/combaine/common/logger"
	"github.com/combaine/combaine/senders"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/process"
	"github.com/sirupsen/logrus"
	grpc "google.golang.org/grpc"
)

var (
	sLock     sync.Mutex
	services  = map[string]*grpc.ClientConn{}
	appsDir   = "/usr/lib/combaine/apps/"
	logDir    = "/var/log/combaine/"
	envPrefix = "COMBAINE_WORKER_SERVICE_"

	servicesBasePort = 10000

	aggregatorService  = "aggregator.py"
	aggConnIdx         int
	aggClientConnMutex sync.Mutex
	aggConnections     []*aggService
)

type aggService struct {
	target string
	conn   *grpc.ClientConn
}

// concurrent write access only on initialization step
func register(name string, conn *grpc.ClientConn) {
	sLock.Lock()
	defer sLock.Unlock()
	services[name] = conn
}

// GetSenderClient return grpc client to locally spawned senders
func GetSenderClient(sType string) (senders.SenderClient, error) {
	if conn, ok := services[sType]; ok {
		return senders.NewSenderClient(conn), nil
	}
	return nil, errors.New("Unknown sender type: " + sType)
}

// NextAggregatorConn return next connection from pool
func NextAggregatorConn() *grpc.ClientConn {
	aggClientConnMutex.Lock()
	defer aggClientConnMutex.Unlock()
	aggConnIdx = (aggConnIdx + 1) % len(aggConnections)
	return aggConnections[aggConnIdx].conn
}

func spawnService(name string, target string, stopCh chan bool) (*grpc.ClientConn, error) {
	log := logrus.WithField("source", "spawnService")
	envServicePrefix := envPrefix + filepath.Base(name)
	envServicePrefix = strings.Replace(envServicePrefix, ".", "_", -1)
	logoutput, found := os.LookupEnv(envServicePrefix + "_LOGOUTPUT")
	if !found {
		logoutput = logDir + filepath.Base(name) + ".log"
	}
	loglevel, found := os.LookupEnv(envServicePrefix + "_LOGLEVEL")
	if !found {
		loglevel = strings.ToLower(*logger.LogLevel)
	}
	go func() {
		log.Infof("Spawn %s", name)
		for {
			select {
			case <-stopCh:
				log.Infof("interrupted")
				return
			default:
				cmd := exec.Command(
					appsDir+name,
					"--endpoint", "[::]"+target,
					"--logoutput", logoutput,
					"--loglevel", loglevel,
				)
				cmd.SysProcAttr = &syscall.SysProcAttr{
					Pdeathsig: syscall.SIGTERM,
				}
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				err := cmd.Run()
				if err != nil {
					log.Errorf("Run failed for %s: %v", name, err)
				}
				log.Infof("Service %s stopped", name)
				time.Sleep(time.Second * 1)
				log.Infof("Respawn %s", name)
			}
		}
	}()
	return dialService(name, target)
}

func dialService(name, addr string) (*grpc.ClientConn, error) {
	options := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(1024*1024*128),
			grpc.MaxCallRecvMsgSize(1024*1024*128),
		),
	}

	var attempts = 20
	for i := 0; i <= attempts; i++ {
		cc, err := grpc.Dial(addr, options...)
		if err != nil {
			logrus.WithField("source", "dialService").
				Warnf("Failed to dial %s: %v", name, err)
			time.Sleep(time.Second * 5)
		} else {
			return cc, nil
		}
	}
	return nil, errors.Errorf("Failed to dial %s (%d times)", name, attempts)
}

func monitorAggServices(name string, memLimit int, stopCh chan bool) {
	log := logrus.WithField("source", "monitorAggServices")
	workerProcess, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		log.Fatalf("worker failed to monitor aggregate service: %v", err)
	}
	checkMemTicker := time.NewTicker(time.Second * 5)
	defer checkMemTicker.Stop()
	for {
		select {
		case <-stopCh:
			log.Infof("interrupted")
			return
		case <-checkMemTicker.C:
			childs, err := workerProcess.Children()
			if err != nil {
				log.Fatalf("worker failed to monitor aggregate service: %v", err)
			}
			for _, child := range childs {
				args, err := child.CmdlineSlice()
				if err != nil {
					log.Warnf("%d query child args: %v", child.Pid, err)
					continue
				}
				if !strings.Contains(strings.Join(args, ","), name) {
					continue
				}
				var ep string
				for idx, val := range args {
					if val == "--endpoint" {
						if idx+1 < len(args) {
							ep = args[idx+1]
						}
						break
					}
				}
				if ep == "" {
					log.Warnf("%d endpoint args not found: %v", child.Pid, args)
					continue
				}
				memInfo, err := child.MemoryInfo()
				if err != nil {
					log.Warnf("%d failed to get memory info: %v", child.Pid, err)
					continue
				}
				if memInfo.RSS > uint64(memLimit*1024*1024) {
					for idx, srv := range aggConnections {
						if strings.HasSuffix(ep, srv.target) {
							log.Infof("service %s:%s eat to many memory (%d Mb), respawn pid=%d", name, ep, memInfo.RSS/1024/1024, child.Pid)
							nextIdx := (idx + 1) % len(aggConnections)

							// release this service
							aggClientConnMutex.Lock()
							aggConnections[idx] = aggConnections[nextIdx]
							aggClientConnMutex.Unlock()

							// inhibit on replace
							time.Sleep(time.Second * 10)
							log.Infof("terminate pid=%d", child.Pid)
							_ = child.Terminate()
							// inhibit on terminate
							time.Sleep(time.Second * 3)
							_ = child.Kill()
							// inhibit on kill
							time.Sleep(time.Second * 3)

							// spawnService should restart process on this endpoint
							acc, err := dialService(name, ep)
							if err != nil {
								log.Fatalf("failed to spawn service %s: %v", name, err)
							}
							_ = srv.conn.Close()
							srv.conn = acc
							// return back
							aggClientConnMutex.Lock()
							aggConnections[idx] = srv
							aggClientConnMutex.Unlock()
							break
						}
					}
				}
			}
		}
	}
}

// SpawnServices spawn subprocesses with services
func SpawnServices(aggNum, memLimit int, stopCh chan bool) error {

	os.Stdout.WriteString("ENVIRON\n")
	for _, s := range os.Environ() {
		os.Stdout.WriteString(s + "\n")
	}

	port := servicesBasePort - aggNum
	aggServiceName := "aggregator/" + aggregatorService
	for idx := 0; idx < aggNum; idx++ {
		target := ":" + strconv.Itoa(port)
		acc, err := spawnService(aggServiceName, target, stopCh)
		if err != nil {
			return err
		}
		aggConnections = append(aggConnections, &aggService{target: target, conn: acc})
		port++
	}
	go monitorAggServices(aggServiceName, memLimit, stopCh)

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
			t := ":" + strconv.Itoa(p)
			sc, err := spawnService(n, t, stopCh)
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
