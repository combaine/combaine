package combainer

import (
	"sync"

	"github.com/noxiouz/Combaine/common/cache"
	"github.com/noxiouz/Combaine/common/tasks"
)

type CloudHostsDelegate func() ([]string, error)

type PTasksMemoryPool struct {
	pool sync.Pool
}

func (p *PTasksMemoryPool) Put(t *tasks.ParsingTask) {
	p.pool.Put(t)
}

func (p *PTasksMemoryPool) Get() *tasks.ParsingTask {
	return p.pool.Get().(*tasks.ParsingTask)
}

func NewPTaskMemoryPool() *PTasksMemoryPool {
	return &PTasksMemoryPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &tasks.ParsingTask{}
			},
		},
	}
}

type AggTasksMemoryPool struct {
	pool sync.Pool
}

func (a *AggTasksMemoryPool) Put(t *tasks.AggregationTask) {
	a.pool.Put(t)
}

func (a *AggTasksMemoryPool) Get() *tasks.AggregationTask {
	return a.pool.Get().(*tasks.AggregationTask)
}

func NewAggTaskMemoryPool() *AggTasksMemoryPool {
	return &AggTasksMemoryPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &tasks.AggregationTask{}
			},
		},
	}
}

type Context struct {
	Cache    cache.Cache
	Hosts    CloudHostsDelegate
	PTasks   *PTasksMemoryPool
	AggTasks *AggTasksMemoryPool
}

func NewContext() *Context {
	return &Context{
		Cache:    nil,
		Hosts:    nil,
		PTasks:   NewPTaskMemoryPool(),
		AggTasks: NewAggTaskMemoryPool(),
	}
}
