package asynq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hibiken/asynq"
	"testing"
	"time"
)

// TestAsynq 测试
func TestAsynq(t *testing.T) {
	conf := &Config{
		Host: "192.168.246.128:6379",
		Pwd:  "123456",
		Db:   0,
	}

	serv := NewAsynq(conf)
	// 启动消费监听
	topic := "test"
	mux := asynq.NewServeMux()
	mux.HandleFunc(topic, func(ctx context.Context, task *asynq.Task) error {
		fmt.Println("consumer type:", task.Type(), "data:", string(task.Payload()))
		return nil
	})
	if err := serv.Consumer.Start(mux); err != nil {
		panic(err)
	}

	go func() {
		// 启动生产
		taskMsg := asynq.NewTask(topic, []byte("hello world"))
		// 延迟发送
		option := asynq.ProcessIn(time.Second * 10)
		info, err := serv.Producer.Enqueue(taskMsg, option)
		if err != nil {
			panic(err)
		}

		data, _ := json.Marshal(info)
		fmt.Println("---------- producer info:", string(data))
	}()

	time.Sleep(time.Second * 30)
}
