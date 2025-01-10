package goroutine_pool

import (
	"sync"
)

var (
	GPool   *TGoroutinePool
	_once   sync.Once
	_onceV2 sync.Once
	poolMap sync.Map
)

// GetPool 默认协程数量500 channelSize 1
func GetPool() *TGoroutinePool {
	_once.Do(func() {
		if GPool == nil {
			GPool = NewPool(500, 1)
			GPool.Run()
		}
	})

	return GPool
}

// GetPoolV2 支持指定名称分组，默认协程数量10个 channelSize 1
func GetPoolV2(name string) *TGoroutinePool {
	_onceV2.Do(func() {
		poolMap = sync.Map{}
	})

	if val, ok := poolMap.Load(name); ok {
		return val.(*TGoroutinePool)
	}

	defaultGoroutineNum := 10
	maxChannelSize := 1
	if name == "" {
		defaultGoroutineNum = 200
	}

	pool := NewPool(defaultGoroutineNum, maxChannelSize)
	pool.Run()
	poolMap.Store(name, pool)
	return pool
}

// GetPoolV3 支持指定名称分组、协程数量
func GetPoolV3(name string, goroutineNum int) *TGoroutinePool {
	_onceV2.Do(func() {
		poolMap = sync.Map{}
	})

	if val, ok := poolMap.Load(name); ok {
		return val.(*TGoroutinePool)
	}

	defaultGoroutineNum := goroutineNum
	maxChannelSize := 1
	if name == "" {
		defaultGoroutineNum = 200
	}

	pool := NewPool(defaultGoroutineNum, maxChannelSize)
	pool.Run()
	poolMap.Store(name, pool)
	return pool
}

// GetPoolV4 支持指定名称分组、协程数量、channel size大小
func GetPoolV4(name string, goroutineNum int, maxChannelSize int) *TGoroutinePool {
	_onceV2.Do(func() {
		poolMap = sync.Map{}
	})

	if val, ok := poolMap.Load(name); ok {
		return val.(*TGoroutinePool)
	}

	defaultGoroutineNum := goroutineNum
	if name == "" {
		defaultGoroutineNum = 200
	}

	pool := NewPool(defaultGoroutineNum, maxChannelSize)
	pool.Run()
	poolMap.Store(name, pool)
	return pool
}
