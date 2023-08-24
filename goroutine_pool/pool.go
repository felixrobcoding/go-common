package goroutine_pool

import (
	"fmt"
	"log"
	"runtime/debug"
)

/**
 * @Title: 协程池
 * @Author: lz
 * @Desc: 协程管理
 * @Version: v0.0.1
 * @Date: 2022/1/10
 */

type (
	CallFunc func(data interface{}) error
	Task     struct {
		Data     interface{}
		CallBack CallFunc
	}
)

type TGoroutinePool struct {
	EntryChannel chan *Task
	JobsChannel  chan *Task
	goroutineNum int
}

// worker 工作方法
func (t *TGoroutinePool) worker(workerId int) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("崩溃： \n", string(debug.Stack()))
			go t.worker(workerId)
		}
	}()

	for task := range t.JobsChannel {
		err := task.CallBack(task.Data)
		if err != nil {
			fmt.Println("task execute failed workId:", workerId, " err:", err)
		}
	}
}

func (t *TGoroutinePool) Push(data interface{}, callBack CallFunc) {
	t.EntryChannel <- &Task{
		Data:     data,
		CallBack: callBack,
	}
}

// PushData 推送数据
func (t *TGoroutinePool) PushData(task *Task) {
	if task != nil {
		t.EntryChannel <- task
	}
}

// Run 协程池运行
func (t *TGoroutinePool) Run() {
	for i := 0; i < t.goroutineNum; i++ {
		go func() {
			t.worker(i)
		}()
	}

	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Println("pool EntryChannel 崩溃：\n", string(debug.Stack()))
			}
		}()

		for task := range t.EntryChannel {
			t.JobsChannel <- task
		}
	}()
}

// Close 协程池关闭
func (t *TGoroutinePool) Close() {
	close(t.EntryChannel)
	close(t.JobsChannel)
}

func NewPool(goroutineNum int, maxChannelSize int) *TGoroutinePool {
	p := &TGoroutinePool{
		EntryChannel: make(chan *Task, maxChannelSize),
		JobsChannel:  make(chan *Task, maxChannelSize),
		goroutineNum: goroutineNum,
	}
	return p
}
