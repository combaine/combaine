package combainer

import (
	"github.com/Combaine/Combaine/common/cache"

	"github.com/Sirupsen/logrus"
)

type CloudHostsDelegate func() ([]string, error)

type Context struct {
	*logrus.Logger
	Cache cache.Cache
	Hosts CloudHostsDelegate
}
