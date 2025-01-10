package retry_handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/felixrobcoding/go-common/asynq"
	"github.com/felixrobcoding/go-common/utiltools"
	"testing"
	"time"
)

type TestData struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestHandler(t *testing.T) {
	startTime := time.Now().Unix()
	retyrHandler := NewRetryHandler()
	// 订阅
	err := retyrHandler.Subscribe(context.Background(), &RetryHandlerConfig{
		Topic:                "test",
		MinRetryTimeInterval: 1 * time.Second,
		MaxRetryTimeInterval: 16 * time.Second,
		TotalRetryTime:       1 * time.Hour * 24,
		MaxRetryCount:        2,
	}, func(ctx context.Context, context []byte) error {
		recvData := &TestData{}
		json.Unmarshal(context, recvData)
		nowUnix := time.Now().Unix()
		fmt.Println(utiltools.ToJson(recvData), " 耗时：", nowUnix-startTime)
		fmt.Println("")
		startTime = nowUnix
		return fmt.Errorf("xxx")
	}, func(ctx context.Context, context []byte) {
		recvData := &TestData{}
		json.Unmarshal(context, recvData)
		fmt.Println(utiltools.ToJson(recvData), " 告警回调")
	})
	if err != nil {
		return
	}

	// 启动
	err = retyrHandler.Start(&asynq.Config{
		"192.168.246.131:6379",
		"",
		0,
	})
	if err != nil {
		return
	}

	startTime = time.Now().Unix()
	retyrHandler.Publisher(context.Background(), "test", &TestData{
		Name: "lizhi",
		Age:  30,
	})

	time.Sleep(time.Second * 1000)
}
