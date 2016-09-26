package cache

import (
	"fmt"
	"sync"

	"github.com/cocaine/cocaine-framework-go/cocaine"
	"github.com/combaine/combaine/common/configs"
)

func init() {
	RegisterCache("InMemory", NewInMemoryCache)
	RegisterCache("Cocaine", NewCocaineCache)
}

type CacheConstructor func(*configs.PluginConfig) (Cache, error)

var (
	factory map[string]CacheConstructor = make(map[string]CacheConstructor)
)

func RegisterCache(name string, f CacheConstructor) {
	_, ok := factory[name]
	if ok {
		panic(fmt.Sprintf("`%s` has been already registered", name))
	}
	factory[name] = f
}

func NewCache(name string, config *configs.PluginConfig) (Cache, error) {
	f, ok := factory[name]
	if ok {
		return f(config)
	}
	return nil, fmt.Errorf("no cache pluign named %s", name)
}

type Cache interface {
	Put(namespace, key string, data []byte) error
	Get(namespace, key string) ([]byte, error)
}

type InMemory struct {
	sync.Mutex
	data map[string][]byte
}

func NewInMemoryCache(config *configs.PluginConfig) (Cache, error) {
	c := &InMemory{
		data: make(map[string][]byte),
	}
	return c, nil
}

func (i *InMemory) Put(namespace, key string, data []byte) error {
	i.Lock()
	defer i.Unlock()
	i.data[namespace+key] = data
	return nil
}

func (i *InMemory) Get(namespace, key string) ([]byte, error) {
	i.Lock()
	defer i.Unlock()
	data, ok := i.data[namespace+key]
	if !ok {
		return nil, fmt.Errorf("No such key `%s` in namespace `%s`", key, namespace)
	}
	return data, nil
}

type CocaineCache struct {
	storage *cocaine.Service
}

func NewCocaineCache(config *configs.PluginConfig) (Cache, error) {
	storage, err := cocaine.NewService("storage")
	if err != nil {
		return nil, err
	}

	c := &CocaineCache{
		storage: storage,
	}

	return c, nil
}

func (c *CocaineCache) Put(namespace, key string, value []byte) error {
	res, ok := <-c.storage.Call("write", namespace, key, []byte(value))
	if !ok {
		return nil
	}
	return res.Err()
}

func (c *CocaineCache) Get(namespace, key string) ([]byte, error) {
	res := <-c.storage.Call("read", namespace, key)

	if err := res.Err(); err != nil {
		return nil, err
	}

	var z []byte
	if err := res.Extract(&z); err != nil {
		return nil, err
	}
	return z, nil
}
