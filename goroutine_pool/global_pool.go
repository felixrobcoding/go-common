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

func GetPool() *TGoroutinePool {
	_once.Do(func() {
		if GPool == nil {
			GPool = NewPool(500, 1)
			GPool.Run()
		}
	})

	return GPool
}

func GetPoolV2(name string) *TGoroutinePool {
	_onceV2.Do(func() {
		poolMap = sync.Map{}
	})

	if val, ok := poolMap.Load(name); ok {
		return val.(*TGoroutinePool)
	}

	defaultGoroutineNum := 10
	maxChannelSize := 100
	if name == "" {
		defaultGoroutineNum = 200
	}

	pool := NewPool(defaultGoroutineNum, maxChannelSize)
	pool.Run()
	poolMap.Store(name, pool)
	return pool
}
