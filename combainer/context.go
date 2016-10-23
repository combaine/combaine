package combainer

import (
	"github.com/combaine/combaine/common/cache"
	"google.golang.org/grpc"

	"github.com/Sirupsen/logrus"
)

type CloudHostsDelegate func() ([]string, error)

type Context struct {
	*logrus.Logger
	Cache    cache.Cache
	Balancer grpc.Balancer
}
