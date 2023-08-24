package work_queue

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

var (
	count       atomic.Int32
	finishCount atomic.Int32
)

func greeting(work *Work) {
	fmt.Printf("%s\n", work.Data)
	finishCount.Add(1)
	time.Sleep(time.Millisecond * 100)
}

func TestWorkQueue(t *testing.T) {
	wq := CreateWorkQueue(1000)
	var greet string

	groutinueNum := 1000
	perExecCount := 100
	since := time.Now()
	for i := 0; i < groutinueNum; i++ {
		go func() {
			for j := 0; j < perExecCount; j++ {
				count.Add(1)
				greet = fmt.Sprintf(" hello everyone i:%v j:%v", i, j)
				work := new(Work)
				work.Data = greet
				work.Action = greeting
				wq.ScheduleWork(work)
			}
		}()
	}

	for {
		select {
		case <-time.After(time.Second * 1):
			if finishCount.Load() == int32(groutinueNum*perExecCount) {
				fmt.Println("--------- count", count.Load(), " 耗时：", time.Since(since), " 执行总次数：", groutinueNum*perExecCount, " 平均耗时：", time.Since(since)/time.Duration(groutinueNum*perExecCount), " qps:", finishCount.Load()/int32(time.Since(since)/time.Second))
				return
			}
		}
	}

}
