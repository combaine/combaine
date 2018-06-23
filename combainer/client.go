package combainer

import (
	"context"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/encoding/gzip"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/hosts"
	"github.com/combaine/combaine/repository"
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
	ID uint64
	clientStats

	conn *grpc.ClientConn
}

// NewClient returns new client
func NewClient() (*Client, error) {
	conn, err := grpc.Dial("serf:///worker",
		grpc.WithInsecure(),
		grpc.WithBalancerName(roundrobin.Name),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)),
	)

	if err != nil {
		return nil, err
	}
	id := common.GenerateClientID()
	return &Client{ID: id, conn: conn}, nil
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

	hostFetcher, err := LoadHostFetcher(parsingConfig.HostFetcher)
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

	log.Info("Session parametrs have been updated successfully")
	return sp, nil
}

// Dispatch does one iteration of tasks dispatching
func (cl *Client) Dispatch(iteration uint64, parsingConfigName string, sessionID string, shouldWait bool) error {
	log := logrus.WithFields(logrus.Fields{
		"iteration": strconv.FormatUint(iteration, 10),
		"session":   sessionID,
		"config":    parsingConfigName})

	params, err := cl.updateSessionParams(parsingConfigName)
	if err != nil {
		return errors.Wrap(err, "update session params")
	}

	log.Info("Start new iteration")

	var wg sync.WaitGroup
	startTime := time.Now()
	// Context for the dispath.  It includes parsing, aggregation and wait stages
	wctx, wcancel := context.WithDeadline(context.Background(), startTime.Add(params.WholeTime))
	defer wcancel()

	// Parsing phase
	var mu sync.Mutex
	pctx, pcancel := context.WithDeadline(wctx, startTime.Add(params.ParsingTime))
	totalTasksAmount := len(params.PTasks)
	log.Infof("Send %d tasks to parsing", totalTasksAmount)
	parsingResult := rpc.ParsingResult{Data: make(map[string][]byte)}
	tokens := make(chan struct{}, params.ParallelParsings)
	for _, task := range params.PTasks {
		// Description of task
		task.Frame.Previous = startTime.Unix()
		task.Frame.Current = startTime.Add(params.WholeTime).Unix()
		task.Id = sessionID

		wg.Add(1)
		tokens <- struct{}{} // acqure
		go func(t rpc.ParsingTask) {
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
		go func(t rpc.AggregatingTask) {
			defer wg.Done()
			cl.doAggregation(wctx, &t)
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

func (cl *Client) doParsing(ctx context.Context, task *rpc.ParsingTask, m *sync.Mutex, r rpc.ParsingResult) {
	log := logrus.WithFields(logrus.Fields{"session": task.Id})

	c := rpc.NewWorkerClient(cl.conn)
	reply, err := c.DoParsing(ctx, task)
	if err != nil {
		log.Errorf("doParsing: reply error: %s", err)
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

func (cl *Client) doAggregation(ctx context.Context, task *rpc.AggregatingTask) {
	log := logrus.WithFields(logrus.Fields{"session": task.Id})

	c := rpc.NewWorkerClient(cl.conn)
	_, err := c.DoAggregating(ctx, task)
	if err != nil {
		log.Errorf("doAggregation: reply error: %s", err)
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
