package combainer

import (
	"github.com/combaine/combaine/combainer/worker"
	"github.com/combaine/combaine/common/cache"

	"github.com/Sirupsen/logrus"
)

type CloudHostsDelegate func() ([]string, error)

type Context struct {
	*logrus.Logger
	Cache    cache.Cache
	Hosts    CloudHostsDelegate
	Resolver worker.Resolver
}
