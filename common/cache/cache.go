package cache

import (
	"fmt"
	"sync"

	"github.com/combaine/combaine/common"
)

func init() {
	RegisterCache("InMemory", NewInMemoryCache)
}

// Constructor type for func that return new cache
type Constructor func(*common.PluginConfig) (Cache, error)

var factory = make(map[string]Constructor)

// RegisterCache perform registration of new cache in cache factory
func RegisterCache(name string, f Constructor) {
	_, ok := factory[name]
	if ok {
		panic(fmt.Sprintf("`%s` has been already registered", name))
	}
	factory[name] = f
}

// NewCache build and return new cache by Constructor from factory
func NewCache(name string, config *common.PluginConfig) (Cache, error) {
	f, ok := factory[name]
	if ok {
		return f(config)
	}
	return nil, fmt.Errorf("no cache pluign named %s", name)
}

// Cache interface that shoud implement cache
type Cache interface {
	Put(namespace, key string, data []byte) error
	Get(namespace, key string) ([]byte, error)
}

// InMemory is a simplest in memory cache
type InMemory struct {
	sync.Mutex
	data map[string][]byte
}

// NewInMemoryCache build and return new instance of InMemory cache
func NewInMemoryCache(config *common.PluginConfig) (Cache, error) {
	c := &InMemory{
		data: make(map[string][]byte),
	}
	return c, nil
}

// Put place data in cache
func (i *InMemory) Put(namespace, key string, data []byte) error {
	i.Lock()
	i.data[namespace+key] = data
	i.Unlock()
	return nil
}

// Get return data from cache
func (i *InMemory) Get(namespace, key string) ([]byte, error) {
	i.Lock()
	defer i.Unlock()
	data, ok := i.data[namespace+key]
	if !ok {
		return nil, fmt.Errorf("No such key `%s` in namespace `%s`", key, namespace)
	}
	return data, nil
}
