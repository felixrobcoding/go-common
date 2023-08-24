package goroutine_pool

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

var (
	finishCount atomic.Int32
)

// 理想值，单个任务执行5毫秒的情况下，线程池开500 goroutine 任务数200000 每秒能处理40000个任务
// 每一个任务执行10毫秒，线程池开500 goroutine 任务数100000
// finishCount 100000  耗时： 3.3772321s  平均耗时： 33.772µs  qps: 33333
func TestGoroutinePool(t *testing.T) {
	since := time.Now()
	total := int32(200000)
	for i := 0; i < int(total); i++ {
		GetPool().Push(i, func(data interface{}) error {
			fmt.Println("hello world i :", data.(int))
			finishCount.Add(1)
			time.Sleep(time.Millisecond * 5)
			return nil
		})
	}

	for {
		select {
		case <-time.After(time.Second * 1):
			if total == finishCount.Load() {
				fmt.Println("--------- finishCount", finishCount.Load(), " 耗时：", time.Since(since), " 平均耗时：", time.Since(since)/time.Duration(finishCount.Load()), " qps:", finishCount.Load()/int32(time.Since(since)/time.Second))
				return
			}
		}
	}
}
