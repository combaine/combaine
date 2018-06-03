package combainer

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/cache"
	"github.com/combaine/combaine/common/hosts"
	"github.com/combaine/combaine/rpc"
)

type sessionParams struct {
	ParallelParsings int
	ParsingTime      time.Duration
	WholeTime        time.Duration
	PTasks           []rpc.ParsingTask
	AggTasks         []rpc.AggregatingTask
}

// Client is a distributor of tasks across the computation grid
type Client struct {
	ID         string
	repository common.Repository
	Cache      cache.Cache
	Log        *logrus.Entry
	clientStats
}

// NewClient returns new client
func NewClient(cache cache.Cache, repo common.Repository) (*Client, error) {
	id := common.GenerateSessionID()

	cl := &Client{
		ID:         id,
		repository: repo,
		Cache:      cache,
		Log:        logrus.WithField("client", id),
	}
	return cl, nil
}

func (cl *Client) updateSessionParams(config string) (sp *sessionParams, err error) {
	cl.Log.WithFields(logrus.Fields{"config": config}).Info("updating session parametrs")

	encodedParsingConfig, err := cl.repository.GetParsingConfig(config)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{"config": config, "error": err}).Error("unable to load .yaml or .json config")
		return nil, err
	}

	var parsingConfig common.ParsingConfig
	if err = encodedParsingConfig.Decode(&parsingConfig); err != nil {
		cl.Log.WithFields(logrus.Fields{"config": config, "error": err}).Error("unable to decode parsingConfig")
		return nil, err
	}

	cfg := cl.repository.GetCombainerConfig()
	parsingConfig.UpdateByCombainerConfig(&cfg)
	aggregationConfigs, err := common.GetAggregationConfigs(cl.repository, &parsingConfig, config)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{"config": config, "error": err}).Error("unable to read aggregation configs")
		return nil, err
	}

	cl.Log.Infof("updating config metahost: %s", parsingConfig.Metahost)

	hostFetcher, err := LoadHostFetcher(cl.Cache, parsingConfig.HostFetcher)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{"config": config, "error": err}).Error("Unable to construct SimpleFetcher")
		return
	}

	allHosts := make(hosts.Hosts)
	for _, item := range parsingConfig.Groups {
		hostsForGroup, err := hostFetcher.Fetch(item)
		if err != nil {
			cl.Log.WithFields(logrus.Fields{"config": config, "error": err, "group": item}).Warn("unable to get hosts")
			continue
		}

		allHosts.Merge(&hostsForGroup)
	}

	listOfHosts := allHosts.AllHosts()

	if len(listOfHosts) == 0 {
		err := errors.New("No hosts in given groups")
		cl.Log.WithFields(logrus.Fields{"config": config, "group": parsingConfig.Groups}).Warn("no hosts in given groups")
		return nil, err
	}

	cl.Log.WithFields(logrus.Fields{"config": config}).Infof("Processing %d hosts in task", len(listOfHosts))
	cl.Log.WithFields(logrus.Fields{"config": config}).Debugf("hosts: %s", listOfHosts)

	parallelParsings := len(listOfHosts)
	if parsingConfig.ParallelParsings > 0 && parallelParsings > parsingConfig.ParallelParsings {
		parallelParsings = parsingConfig.ParallelParsings
	}

	// TODO: replace with more effective tinylib/msgp
	packedParsingConfig, _ := common.Pack(parsingConfig)
	packedAggregationConfigs, _ := common.Pack(aggregationConfigs)
	packedHosts, _ := common.Pack(allHosts)

	// Tasks for parsing
	pTasks := make([]rpc.ParsingTask, len(listOfHosts))
	for idx, host := range listOfHosts {
		pTasks[idx] = rpc.ParsingTask{
			Frame:                     new(rpc.TimeFrame),
			Host:                      host,
			ParsingConfigName:         config,
			EncodedParsingConfig:      packedParsingConfig,
			EncodedAggregationConfigs: packedAggregationConfigs,
		}
	}

	aggTasks := make([]rpc.AggregatingTask, len(parsingConfig.AggConfigs))
	for idx, name := range parsingConfig.AggConfigs {
		packedAggregationConfig, _ := common.Pack((*aggregationConfigs)[name])
		aggTasks[idx] = rpc.AggregatingTask{
			Frame:                    new(rpc.TimeFrame),
			Config:                   name,
			ParsingConfigName:        config,
			EncodedParsingConfig:     packedParsingConfig,
			EncodedAggregationConfig: packedAggregationConfig,
			EncodedHosts:             packedHosts,
		}
	}

	parsingTime, wholeTime := generateSessionTimeFrame(parsingConfig.IterationDuration)

	sp = &sessionParams{
		ParallelParsings: parallelParsings,
		ParsingTime:      parsingTime,
		WholeTime:        wholeTime,
		PTasks:           pTasks,
		AggTasks:         aggTasks,
	}

	cl.Log.WithField("config", config).Info("Session parametrs have been updated successfully")
	return sp, nil
}

// Dispatch does one iteration of tasks dispatching
func (cl *Client) Dispatch(iteration string, hosts []string, parsingConfigName string, uniqueID string, shouldWait bool) error {
	if uniqueID == "" {
		uniqueID = common.GenerateSessionID()
	}

	dispatchFields := logrus.Fields{
		"iteration": iteration,
		"session":   uniqueID,
		"config":    parsingConfigName}

	sessionParameters, err := cl.updateSessionParams(parsingConfigName)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{"session": uniqueID, "config": parsingConfigName, "error": err}).Error("unable to update session parametrs")
		return err
	}

	startTime := time.Now()
	// Context for the whole dispath.
	// It includes parsing, aggregation and wait stages
	wctx, cancelFunc := context.WithDeadline(context.TODO(), startTime.Add(sessionParameters.WholeTime))
	defer cancelFunc()

	cl.Log.WithFields(dispatchFields).Info("Start new iteration")
	if len(hosts) == 0 {
		cl.Log.WithFields(
			logrus.Fields{"session": uniqueID, "config": parsingConfigName},
		).Error("unable to get (or empty) the list of the cloud hosts")
		return common.ErrNoHosts
	}
	cl.Log.WithFields(
		logrus.Fields{"session": uniqueID, "config": parsingConfigName},
	).Debugf("Dispatch task to cloud hosts: %s", hosts)

	// Parsing phase
	totalTasksAmount := len(sessionParameters.PTasks)
	tokens := make(chan struct{}, sessionParameters.ParallelParsings)
	parsingResult := rpc.ParsingResult{Data: make(map[string][]byte)}
	var mu sync.Mutex
	pctx, cancelFunc := context.WithDeadline(wctx, startTime.Add(sessionParameters.ParsingTime))
	defer cancelFunc()

	cl.Log.WithFields(dispatchFields).Infof("Send %d tasks to parsing", totalTasksAmount)
	var wg sync.WaitGroup
	for _, task := range sessionParameters.PTasks {
		// Description of task
		task.Frame.Previous = startTime.Unix()
		task.Frame.Current = startTime.Add(sessionParameters.WholeTime).Unix()
		task.Id = uniqueID

		wg.Add(1)
		tokens <- struct{}{} // acqure
		go func(t rpc.ParsingTask) {
			defer wg.Done()
			defer func() { <-tokens }() // release
			cl.doParsing(pctx, &t, &mu, hosts, parsingResult)
		}(task)
	}
	wg.Wait()
	cl.Log.WithFields(dispatchFields).Infof("Parsing finished for %d hosts", len(parsingResult.Data))

	// Aggregation phase
	totalTasksAmount = len(sessionParameters.AggTasks)
	cl.Log.WithFields(dispatchFields).Infof("Send %d tasks to aggregate", totalTasksAmount)
	for _, task := range sessionParameters.AggTasks {
		task.Frame.Previous = startTime.Unix()
		task.Frame.Current = startTime.Add(sessionParameters.WholeTime).Unix()
		task.Id = uniqueID
		task.ParsingResult = &parsingResult

		wg.Add(1)
		go func(t rpc.AggregatingTask) {
			defer wg.Done()
			cl.doAggregationHandler(wctx, &t, hosts)
		}(task)
	}
	wg.Wait()

	cl.Log.WithFields(dispatchFields).Info("Aggregation has finished")

	// Wait for next iteration if needed.
	// wctx has a deadline
	if shouldWait {
		<-wctx.Done()
	}

	cl.Log.WithFields(dispatchFields).Debug("Go to the next iteration")

	return nil
}

func dialContext(ctx context.Context, hosts []string) (conn *grpc.ClientConn, err error) {
	hostsLen := len(hosts)
	if hostsLen == 0 {
		return nil, errors.New("empty list of hosts")
	}
	dialTimeout := 1 * time.Second
	deadline, ok := ctx.Deadline()
	if ok {
		timeout := deadline.Sub(time.Now()) / 2
		timeout = time.Duration(timeout.Nanoseconds() / int64(hostsLen))
		if timeout > time.Millisecond*500 {
			dialTimeout = timeout
		}
	}
	for _, idx := range rand.Perm(hostsLen) {
		// TODO: port must be got from autodiscovery
		address := hosts[idx] + ":10052"

		tctx, tcancel := context.WithTimeout(ctx, dialTimeout)
		conn, err = grpc.DialContext(tctx, address,
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithCompressor(grpc.NewGZIPCompressor()),
			grpc.WithDecompressor(grpc.NewGZIPDecompressor()),
		)
		tcancel()
		switch err {
		case nil:
			return conn, nil
		default:
		}
		// NOTE: to be sure that DialContext return parent context's errors
		if parentErr := ctx.Err(); parentErr != nil {
			err = parentErr
			break
		}
	}
	if conn != nil {
		conn.Close()
		conn = nil
	}
	return nil, err
}

func (cl *Client) doParsing(ctx context.Context, task *rpc.ParsingTask, m *sync.Mutex, hosts []string, r rpc.ParsingResult) {
	conn, err := dialContext(ctx, hosts)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{"session": task.Id, "error": err, "appname": "doParsing"}).Error("grpcDial error")
		cl.clientStats.AddFailedParsing()
		return
	}
	defer conn.Close()

	c := rpc.NewWorkerClient(conn)
	reply, err := c.DoParsing(ctx, task)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{"session": task.Id, "error": err, "appname": "doParsing"}).Error("reply error")
		cl.clientStats.AddFailedParsing()
		return
	}

	m.Lock()
	for k, v := range reply.Data {
		r.Data[k] = v
	}
	m.Unlock()
	cl.clientStats.AddSuccessParsing()

}

func (cl *Client) doAggregationHandler(ctx context.Context, task *rpc.AggregatingTask, hosts []string) {
	conn, err := dialContext(ctx, hosts)
	if err != nil {
		cl.Log.WithFields(
			logrus.Fields{"session": task.Id, "error": err, "appname": "doAggregationHandler"},
		).Error("grpcDial error")
		cl.clientStats.AddFailedAggregate()
		return
	}
	defer conn.Close()
	c := rpc.NewWorkerClient(conn)

	_, err = c.DoAggregating(ctx, task)
	if err != nil {
		cl.Log.WithFields(
			logrus.Fields{"session": task.Id, "error": err, "appname": "doAggregationHandler"},
		).Error("reply error")
		cl.clientStats.AddFailedAggregate()
		return
	}
	cl.clientStats.AddSuccessAggregate()
}

func generateSessionTimeFrame(sessionDuration uint) (time.Duration, time.Duration) {
	parsingTime := time.Duration(float64(sessionDuration)*0.8) * time.Second
	wholeTime := time.Duration(sessionDuration) * time.Second
	return parsingTime, wholeTime
}
