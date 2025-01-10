package redis

import (
	"context"
	"cp.wjaiot.net/cloud-server/smarlife/SaaSBP/go-common/utiltools"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"testing"
	"time"
)

func StreamSub2() {
	defer utiltools.ExceptionCatch()
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

	pubsub := NewStreamPubSub(client, "test_group")
	pubsub.RegisterHandler("topic_test", func(uniqueIds []string, ops string, data []byte) {
		fmt.Println("subscribe2 uniqueIds:", utiltools.ToJson(uniqueIds), " ops:", ops, "data:", string(data))
	})

	pubsub.SubscriberPublisher()

	time.Sleep(time.Second * 1200)
	return
}

func TestStreamPubSub(t *testing.T) {
	defer utiltools.ExceptionCatch()
	client := redis.NewClient(&redis.Options{
		Addr:       "192.168.246.128:6379",
		Password:   "",
		DB:         0,
		PoolSize:   30,
		MaxRetries: 5,
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		log.Println(err)
	}

	pubSub := NewStreamPubSub(client, "test_group")
	pubSub.RegisterHandler("topic_test", func(uniqueIds []string, ops string, data []byte) {
		fmt.Println("subscribe1 uniqueIds:", utiltools.ToJson(uniqueIds), " ops:", ops, "data:", string(data))
	})

	pubSub.SubscriberPublisher()
	go StreamSub2()
	time.Sleep(time.Second * 1)

	for i := 0; i < 10; i++ {
		pubSub.PublisherMessage("topic_test", []string{"0"}, "login", fmt.Sprintf("hello world i:%v", i), false)
		time.Sleep(time.Second * 1)
	}

	time.Sleep(time.Second * 12000)

	return
}
