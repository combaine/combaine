package combainer

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/Sirupsen/logrus"

	"github.com/combaine/combaine/common"
	"github.com/combaine/combaine/common/configs"
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
	Repository configs.Repository
	*Context
	Log *logrus.Entry
	clientStats

	conn *grpc.ClientConn
}

// NewClient returns new client
func NewClient(context *Context, repo configs.Repository) (*Client, error) {
	id := GenerateSessionId()
	conn, err := grpc.Dial("worker",
		grpc.WithBalancer(context.Balancer),
		grpc.WithInsecure(),
		grpc.WithCompressor(grpc.NewGZIPCompressor()),
		grpc.WithDecompressor(grpc.NewGZIPDecompressor()))
	if err != nil {
		return nil, err
	}
	cl := &Client{
		ID:         id,
		Repository: repo,
		Context:    context,
		Log:        context.Logger.WithField("client", id),

		conn: conn,
	}
	return cl, nil
}

// Close releases grpc.ClientConn
func (cl *Client) Close() error {
	return cl.conn.Close()
}

func (cl *Client) updateSessionParams(config string) (sp *sessionParams, err error) {
	cl.Log.WithFields(logrus.Fields{"config": config}).Info("updating session parametrs")

	var (
		// tasks
		pTasks   []rpc.ParsingTask
		aggTasks []rpc.AggregatingTask

		// timeouts
		parsingTime time.Duration
		wholeTime   time.Duration
	)

	encodedParsingConfig, err := cl.Repository.GetParsingConfig(config)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{"config": config, "error": err}).Error("unable to load config")
		return nil, err
	}

	var parsingConfig configs.ParsingConfig
	if err = encodedParsingConfig.Decode(&parsingConfig); err != nil {
		cl.Log.WithFields(logrus.Fields{"config": config, "error": err}).Error("unable to decode parsingConfig")
		return nil, err
	}

	cfg := cl.Repository.GetCombainerConfig()
	parsingConfig.UpdateByCombainerConfig(&cfg)
	aggregationConfigs, err := GetAggregationConfigs(cl.Repository, &parsingConfig)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{"config": config, "error": err}).Error("unable to read aggregation configs")
		return nil, err
	}

	cl.Log.Infof("updating config: group %s, metahost %s",
		parsingConfig.GetGroup(), parsingConfig.GetMetahost())

	hostFetcher, err := LoadHostFetcher(cl.Context, parsingConfig.HostFetcher)
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
		err := fmt.Errorf("No hosts in given groups")
		cl.Log.WithFields(logrus.Fields{"config": config, "group": parsingConfig.Groups}).Warn("no hosts in given groups")
		return nil, err
	}

	cl.Log.WithFields(logrus.Fields{"config": config}).Infof("hosts: %s", listOfHosts)

	parallelParsings := len(listOfHosts)
	if parsingConfig.ParallelParsings > 0 && parallelParsings > parsingConfig.ParallelParsings {
		parallelParsings = parsingConfig.ParallelParsings
	}

	// TODO: replace with more effective tinylib/msgp
	packedParsingConfig, _ := common.Pack(parsingConfig)
	packedAggregationConfigs, _ := common.Pack(aggregationConfigs)
	packedHosts, _ := common.Pack(allHosts)

	// Tasks for parsing
	for _, host := range listOfHosts {
		pTasks = append(pTasks, rpc.ParsingTask{
			Frame:                     new(rpc.TimeFrame),
			Host:                      host,
			ParsingConfigName:         config,
			EncodedParsingConfig:      packedParsingConfig,
			EncodedAggregationConfigs: packedAggregationConfigs,
		})
	}

	for _, name := range parsingConfig.AggConfigs {
		packedAggregationConfig, _ := common.Pack((*aggregationConfigs)[name])
		aggTasks = append(aggTasks, rpc.AggregatingTask{
			Frame:                    new(rpc.TimeFrame),
			Config:                   name,
			ParsingConfigName:        config,
			EncodedParsingConfig:     packedParsingConfig,
			EncodedAggregationConfig: packedAggregationConfig,
			EncodedHosts:             packedHosts,
		})
	}

	parsingTime, wholeTime = GenerateSessionTimeFrame(parsingConfig.IterationDuration)

	sp = &sessionParams{
		ParallelParsings: parallelParsings,
		ParsingTime:      parsingTime,
		WholeTime:        wholeTime,
		PTasks:           pTasks,
		AggTasks:         aggTasks,
	}

	cl.Log.Info("Session parametrs have been updated successfully")
	cl.Log.WithFields(logrus.Fields{"config": config}).Debugf("Current session parametrs. %q", sp)

	return sp, nil
}

// Dispatch does one iteration of tasks dispatching
func (cl *Client) Dispatch(parsingConfigName string, uniqueID string, shouldWait bool) error {
	GlobalObserver.RegisterClient(cl, parsingConfigName)
	defer GlobalObserver.UnregisterClient(parsingConfigName)

	if uniqueID == "" {
		uniqueID = GenerateSessionId()
	}

	contextFields := logrus.Fields{
		"session": uniqueID,
		"config":  parsingConfigName}

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

	cl.Log.WithFields(contextFields).Info("Start new iteration")
	// Parsing phase
	totalTasksAmount := len(sessionParameters.PTasks)
	tokens := make(chan struct{}, sessionParameters.ParallelParsings)
	parsingResult := rpc.ParsingResult{Data: make(map[string][]byte)}
	var mu sync.Mutex
	pctx, cancelFunc := context.WithDeadline(wctx, startTime.Add(sessionParameters.ParsingTime))
	defer cancelFunc()

	cl.Log.WithFields(contextFields).Infof("Send %d tasks to parsing", totalTasksAmount)
	var wg sync.WaitGroup
	for i, task := range sessionParameters.PTasks {
		// Description of task
		task.Frame.Previous = startTime.Unix()
		task.Frame.Current = startTime.Add(sessionParameters.WholeTime).Unix()
		task.Id = uniqueID

		cl.Log.WithFields(contextFields).Debugf("Send task number %d/%d to parsing, content: %q", i+1, totalTasksAmount, task)

		wg.Add(1)
		tokens <- struct{}{} // acqure
		go func(t rpc.ParsingTask) {
			defer wg.Done()
			defer func() { <-tokens }() // release
			cl.doParsing(pctx, &t, &mu, parsingResult)
		}(task)
	}
	wg.Wait()
	cl.Log.WithFields(contextFields).Infof("Parsing finished for %d hosts", len(parsingResult.Data))

	// Aggregation phase
	totalTasksAmount = len(sessionParameters.AggTasks)
	cl.Log.WithFields(contextFields).Infof("Send %d tasks to aggregate", totalTasksAmount)
	for i, task := range sessionParameters.AggTasks {
		task.Frame.Previous = startTime.Unix()
		task.Frame.Current = startTime.Add(sessionParameters.WholeTime).Unix()
		task.Id = uniqueID
		task.ParsingResult = &parsingResult

		cl.Log.WithFields(contextFields).Debugf("Send task number %d/%d to aggregate, content: %q", i+1, totalTasksAmount, task)
		wg.Add(1)
		go func(t rpc.AggregatingTask) {
			defer wg.Done()
			cl.doAggregationHandler(wctx, &t)
		}(task)
	}
	wg.Wait()

	cl.Log.WithFields(contextFields).Info("Aggregation has finished")

	// Wait for next iteration if needed.
	// wctx has a deadline
	if shouldWait {
		<-wctx.Done()
	}

	cl.Log.WithFields(contextFields).Debug("Go to the next iteration")

	return nil
}

func (cl *Client) doParsing(ctx context.Context, task *rpc.ParsingTask, m *sync.Mutex, r rpc.ParsingResult) {
	c := rpc.NewWorkerClient(cl.conn)
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

func (cl *Client) doAggregationHandler(ctx context.Context, task *rpc.AggregatingTask) {
	c := rpc.NewWorkerClient(cl.conn)
	_, err := c.DoAggregating(ctx, task)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{"session": task.Id, "error": err, "appname": "doAggregationHandler"}).Error("reply error")
		cl.clientStats.AddFailedAggregate()
		return
	}
	cl.clientStats.AddSuccessAggregate()
}
