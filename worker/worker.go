package worker

import "github.com/combaine/combaine/common/cache"

var cacher cache.ServiceCacher

// InitializeServiceCacher initialize cocaine service cacher
// it cache only localhost connections to the cocaine
func InitializeServiceCacher() {
	cacher = cache.NewServiceCacher(cache.NewServiceWithLocalLogger)
}
