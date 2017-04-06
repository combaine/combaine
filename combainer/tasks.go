package combainer

import (
	"strconv"
	"time"
)

var (
	shouldWait  = true
	genUniqueID = ""
)

func (c *Cluster) distributeTasks() error {
	// TODO configs, err := c.repo.ListParsingConfigs()
	return nil
}

func (c *fsm) handleTask(config string, stopCh chan struct{}) {
	var iteration uint64
	log := c.log.WithField("config", config)

RECLIENT:
	cl, err := NewClient(c.cache, c.repo)
	if err != nil {
		log.Errorf("can't create client %s", err)
		time.Sleep(c.updateInterval)
		goto RECLIENT
	}
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		iteration++
		ilog := log.WithField("iteration", strconv.FormatUint(iteration, 10))
		hosts := (*Cluster)(c).Hosts()
		if err = cl.Dispatch(hosts, config, genUniqueID, shouldWait); err != nil {
			ilog.Errorf("Dispatch error %s", err)
			time.Sleep(c.updateInterval)
		}
	}
}
