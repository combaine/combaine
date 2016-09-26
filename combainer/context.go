package combainer

import (
	"github.com/combaine/combaine/common/cache"
	"github.com/hashicorp/serf/serf"

	"github.com/Sirupsen/logrus"
)

type CloudHostsDelegate func() ([]string, error)

type Context struct {
	*logrus.Logger
	Cache cache.Cache
	Serf  *serf.Serf
}
