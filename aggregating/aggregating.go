package aggregating

import (
	"github.com/noxiouz/Combaine/common/logger"
	"github.com/noxiouz/Combaine/common/servicecacher"
	"github.com/noxiouz/Combaine/common/tasks"
)

const (
	storageServiceName = "elliptics"
)

var (
	//storage *cocaine.Service     = logger.MustCreateService(storageServiceName)
	cacher servicecacher.Cacher = servicecacher.NewCacher()
)

func Aggregating(task *tasks.AggregationTask) error {
	logger.Infof("%s start aggregating", task.Id)
	logger.Debugf("%s aggregation config: %s", task.Id, task.AggregationConfig)

	logger.Debugf("%s aggregate hosts: %v", task.Id, task.Hosts)

	logger.Infof("%s Done", task.Id)
	return nil
}
