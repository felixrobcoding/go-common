package asynq

import (
	"context"
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
	serv.SubscribeConsumer(topic, func(ctx context.Context, task *asynq.Task) error {
		fmt.Println("consumer type:", task.Type(), "data:", string(task.Payload()))
		return nil
	})

	err := serv.Start()
	if err != nil {
		panic(err)
	}

	go func() {
		err = serv.Publisher(topic, "hello world", time.Second*10)
		if err != nil {
			panic(err)
		}
	}()

	time.Sleep(time.Second * 30)
}
