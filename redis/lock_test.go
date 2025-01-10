package redis

import (
	"context"
	"cp.wjaiot.net/cloud-server/smarlife/SaaSBP/go-common/utiltools"
	"fmt"
	"github.com/go-redis/redis/v8"
	"testing"
	"time"
)

func TestLock(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:       "192.168.246.128:6379",
		Password:   "",
		DB:         0,
		PoolSize:   30,
		MaxRetries: 5,
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		go func(index int) {
			// 上锁
			redisLock := &TRedLock{}
			redisLock.Lock(client, "test_key", time.Second*10)
			defer redisLock.UnLock(client)
			fmt.Println("----------- index:", index, "time:", utiltools.TimeFormat(time.Now()))
			time.Sleep(time.Second * 2)
		}(i)
	}

	time.Sleep(time.Second * 30)

}
