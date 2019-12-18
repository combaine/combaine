package combainer

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/hosts"
	"github.com/combaine/combaine/repository"
	"github.com/combaine/combaine/utils"
	"github.com/combaine/combaine/worker"
)

// global ClientID counter
var clientID uint64

type sessionParams struct {
	aggregateLocally bool
	ParallelParsings int
	ParsingTime      time.Duration
	WholeTime        time.Duration
	PTasks           []worker.ParsingTask
	AggTasks         []worker.AggregatingTask
}

// Client is a distributor of tasks across the computation grid
type Client struct {
	ID uint64
	clientStats
	log *logrus.Logger

	conn    *grpc.ClientConn
	aggConn *grpc.ClientConn
}

func generateClientID() uint64 {
	return atomic.AddUint64(&clientID, 1)
}

// NewClient returns new client
func NewClient(opt ...func(*Client) error) (*Client, error) {
	conn, err := grpc.Dial("serf:///worker",
		grpc.WithInsecure(),
		grpc.WithBalancerName(roundrobin.Name),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                15 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)),
		grpc.WithDefaultCallOptions(grpc.FailFast(false)),
	)
	if err != nil {
		return nil, err
	}
	aggConn, err := grpc.Dial("passthrough:///[::1]:"+defaultPort, /* local worker */
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*256 /* MB */)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(1024*1024*256 /* MB */)),
		grpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	id := generateClientID()
	c := &Client{ID: id, conn: conn, aggConn: aggConn}

	for _, f := range opt {
		err = f(c)
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

// Close relases grpc.ClientConn
func (cl *Client) Close() error {
	return cl.conn.Close()
}

func (cl *Client) updateSessionParams(config string) (sp *sessionParams, err error) {
	log := logrus.WithField("config", config)

	log.Info("updating session parametrs")

	encodedParsingConfig, err := repository.GetParsingConfig(config)
	if err != nil {
		log.Errorf("unable to load .yaml or .json config: %s", err)
		return nil, err
	}

	var parsingConfig repository.ParsingConfig
	if err = encodedParsingConfig.Decode(&parsingConfig); err != nil {
		log.Errorf("unable to decode parsingConfig: %s", err)
		return nil, err
	}

	cfg := repository.GetCombainerConfig()
	parsingConfig.UpdateByCombainerConfig(&cfg)
	aggregationConfigs, err := repository.GetAggregationConfigs(&parsingConfig, config)
	if err != nil {
		log.Errorf("unable to read aggregation configs: %s", err)
		return nil, err
	}

	log.Infof("updating config metahost: %s", parsingConfig.Metahost)

	hostFetcher, err := common.LoadHostFetcherWithCache(parsingConfig.HostFetcher, combainerCache)
	if err != nil {
		log.Errorf("Unable to construct SimpleFetcher: %s", err)
		return
	}

	allHosts := make(hosts.Hosts)
	for _, item := range parsingConfig.Groups {
		hostsForGroup, err := hostFetcher.Fetch(item)
		if err != nil {
			log.WithFields(logrus.Fields{"error": err, "group": item}).Warn("unable to get hosts")
			continue
		}
		allHosts.Merge(&hostsForGroup)
	}

	listOfHosts := allHosts.AllHosts()

	if len(listOfHosts) == 0 {
		return nil, errors.New("updateSessionParams: No hosts in given groups")
	}

	log.Infof("updateSessionParams: Processing %d hosts in task", len(listOfHosts))
	log.Debugf("updateSessionParams: hosts: %s", listOfHosts)

	parallelParsings := len(listOfHosts)
	if parsingConfig.ParallelParsings > 0 && parallelParsings > parsingConfig.ParallelParsings {
		parallelParsings = parsingConfig.ParallelParsings
	}

	// TODO: replace with more effective tinylib/msgp
	packedParsingConfig, _ := utils.Pack(parsingConfig)
	packedAggregationConfigs, _ := utils.Pack(aggregationConfigs)
	packedHosts, _ := utils.Pack(allHosts)

	// Tasks for parsing
	pTasks := make([]worker.ParsingTask, len(listOfHosts))
	for idx, host := range listOfHosts {
		pTasks[idx] = worker.ParsingTask{
			Frame:                     new(worker.TimeFrame),
			Host:                      host,
			ParsingConfigName:         config,
			EncodedParsingConfig:      packedParsingConfig,
			EncodedAggregationConfigs: packedAggregationConfigs,
		}
	}

	aggTasks := make([]worker.AggregatingTask, len(parsingConfig.AggConfigs))
	for idx, name := range parsingConfig.AggConfigs {
		packedAggregationConfig, _ := utils.Pack((*aggregationConfigs)[name])
		aggTasks[idx] = worker.AggregatingTask{
			Frame:                    new(worker.TimeFrame),
			Config:                   name,
			ParsingConfigName:        config,
			EncodedParsingConfig:     packedParsingConfig,
			EncodedAggregationConfig: packedAggregationConfig,
			EncodedHosts:             packedHosts,
		}
	}

	parsingTime, wholeTime := generateSessionTimeFrame(parsingConfig.IterationDuration)

	sp = &sessionParams{
		aggregateLocally: parsingConfig.DistributeAggregation != "cluster",
		ParallelParsings: parallelParsings,
		ParsingTime:      parsingTime,
		WholeTime:        wholeTime,
		PTasks:           pTasks,
		AggTasks:         aggTasks,
	}

	log.Info("Session parametrs have been updated successfully")
	return sp, nil
}

// Dispatch does one iteration of tasks dispatching
func (cl *Client) Dispatch(iteration uint64, parsingConfigName string, shouldWait bool) error {
	startTime := time.Now()
	sessionID := utils.GenerateSessionID()

	logger := cl.log
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	log := logger.WithFields(logrus.Fields{
		"iteration": strconv.FormatUint(iteration, 10),
		"session":   sessionID,
		"config":    parsingConfigName})

	log.Info("Start new iteration")

	params, err := cl.updateSessionParams(parsingConfigName)
	if err != nil {
		return errors.Wrap(err, "update session params, sessionID: "+sessionID)
	}

	// Context for the dispath.  It includes parsing, aggregation and wait stages
	wctx, wcancel := context.WithDeadline(context.Background(), startTime.Add(params.WholeTime))
	defer wcancel()

	// Parsing phase
	pctx, pcancel := context.WithDeadline(wctx, startTime.Add(params.ParsingTime))
	totalTasksAmount := len(params.PTasks)
	log.Infof("Send %d tasks to parsing", totalTasksAmount)
	parsingResult := worker.ParsingResult{Data: make(map[string][]byte)}
	tokens := make(chan struct{}, params.ParallelParsings)

	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, task := range params.PTasks {
		// Description of task
		task.Frame.Previous = startTime.Unix()
		task.Frame.Current = startTime.Add(params.WholeTime).Unix()
		task.Id = sessionID

		wg.Add(1)
		tokens <- struct{}{} // acqure
		go func(t worker.ParsingTask) {
			defer wg.Done()
			defer func() { <-tokens }() // release
			cl.doParsing(pctx, &t, &mu, parsingResult)
		}(task)
	}
	wg.Wait()
	pcancel()
	log.Infof("Parsing finished for %d hosts", len(parsingResult.Data))

	// Aggregation phase
	totalTasksAmount = len(params.AggTasks)
	log.Infof("Send %d tasks to aggregate", totalTasksAmount)
	for _, task := range params.AggTasks {
		task.Frame.Previous = startTime.Unix()
		task.Frame.Current = startTime.Add(params.WholeTime).Unix()
		task.Id = sessionID
		task.ParsingResult = &parsingResult

		wg.Add(1)
		go func(t worker.AggregatingTask) {
			defer wg.Done()
			cl.doAggregation(wctx, &t, params.aggregateLocally)
		}(task)
	}
	wg.Wait()
	log.Info("Aggregation has finished")

	// Wait for next iteration if needed.
	// wctx has a deadline
	if shouldWait {
		<-wctx.Done()
	}
	log.Debug("Go to the next iteration")

	return nil
}

func (cl *Client) doParsing(ctx context.Context, task *worker.ParsingTask, m *sync.Mutex, r worker.ParsingResult) {
	log := logrus.WithFields(logrus.Fields{"session": task.Id})

	c := worker.NewWorkerClient(cl.conn)
	var remote peer.Peer
	reply, err := c.DoParsing(ctx, task, grpc.Peer(&remote))
	if err != nil {
		log.Errorf("doParsing: reply error from %v: %s", remote.Addr, err)
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

func (cl *Client) doAggregation(ctx context.Context, task *worker.AggregatingTask, local bool) {
	log := logrus.WithFields(logrus.Fields{"session": task.Id})

	conn := cl.conn
	if local {
		log.Debug("doAggregation: locally")
		conn = cl.aggConn
	}
	c := worker.NewWorkerClient(conn)
	var remote peer.Peer
	_, err := c.DoAggregating(ctx, task, grpc.Peer(&remote))
	if err != nil {
		log.Errorf("doAggregation: reply error from %v: %s", remote.Addr, err)
		cl.clientStats.AddFailedAggregate()
		return
	}
	cl.clientStats.AddSuccessAggregate()
}

func generateSessionTimeFrame(sessionDuration uint) (time.Duration, time.Duration) {
	// TODO make parsing time percentage configurable
	parsingTime := time.Duration(float64(sessionDuration)*0.7) * time.Second
	wholeTime := time.Duration(sessionDuration) * time.Second
	return parsingTime, wholeTime
}
