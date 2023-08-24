package snowflake

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

/*
 * 雪花ID生成器，压测20万 将近一秒钟，没有冲突的ID
 */
func TestSnowflake(t *testing.T) {
	var wg sync.WaitGroup
	s, err := NewSnowflake(int64(0), int64(0))
	if err != nil {
		log.Println(err)
		return
	}
	var count atomic.Int32
	var check sync.Map
	t1 := time.Now()
	for i := 0; i < 200000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val := s.NextVal()
			if _, ok := check.Load(val); ok {
				// id冲突检查
				log.Println(fmt.Errorf("error#unique: val:%v", val))
				return
			}
			count.Add(1)
			check.Store(val, 0)
			if val == 0 {
				log.Println(fmt.Errorf("error"))
				return
			}

		}()
	}
	wg.Wait()
	elapsed := time.Since(t1)
	log.Println("generate 20k ids elapsed:", elapsed)
}
