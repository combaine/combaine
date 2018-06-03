package cache

import (
	"errors"
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
		panic("`" + name + "` has been already registered")
	}
	factory[name] = f
}

// NewCache build and return new cache by Constructor from factory
func NewCache(config *common.PluginConfig) (Cache, error) {
	cacheType := "InMemory" // default
	if config != nil {
		var err error
		cacheType, err = config.Type()
		if err != nil {
			return nil, err
		}
	}

	f, ok := factory[cacheType]
	if ok {
		return f(config)
	}
	return nil, errors.New("no cache pluign named " + cacheType)
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
		return nil, errors.New("No such key `" + key + "` in namespace `" + namespace + "`")
	}
	return data, nil
}
