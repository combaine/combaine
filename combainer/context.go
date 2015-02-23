package combainer

import (
	"github.com/noxiouz/Combaine/common/cache"
)

type CloudHostsDelegate func() ([]string, error)

type Context struct {
	Cache cache.Cache
	Hosts CloudHostsDelegate
}
