package event_bus

import (
	"context"
	"cp.wjaiot.net/cloud-server/smarlife/SaaSBP/go-common/utiltools"
	"fmt"
	"go-micro.dev/v4/metadata"
	"sync/atomic"
	"testing"
	"time"
)

type Student struct {
	Name string `json:"name"`
}

// 这种方式可以初始化所有支持的事件总线类型，直接通过枚举类型区别调用即可，推荐使用这种方式
func TestEventBusMgr(t *testing.T) {
	// 启动事件
	err := StartEventBus(&EventBusConfig{
		NormalKafkaBus: &KafkaEventBusConfig{
			Hosts:          []string{"159.75.112.37:9092"},
			IsNewestOffset: true,
		},
		GroupKafkaBus: &KafkaEventBusConfig{
			Hosts:          []string{"159.75.112.37:9092"},
			IsNewestOffset: false,
		},
		RedisBus: &RedisConfig{
			Addr:     "134.175.211.197:23679",
			Password: "wja@2023",
			DB:       0,
		},
	}, "test-server", []string{"test"})
	if err != nil {
		return
	}

	defer StopEventBus()

	// 订阅事件
	eventBusType := EnRedisBus
	var consumerCount atomic.Int32
	var consumerStart = time.Now()
	eventId := "event_user_login"
	GetEventBus(eventBusType).SubscribeEvent(eventId, "test", func(ctx context.Context, event, eventType string, data []byte, src string) error {
		fmt.Println("eventId:", event, " data:", string(data), " src:", src)
		consumerCount.Add(1)
		fmt.Println(" 消费-------- count:", consumerCount.Load(), " 耗时：", time.Since(consumerStart))
		return nil
	})
	defer GetEventBus(eventBusType).UnsubscribeEvent(eventId, "test")

	timeStart := time.Now()
	var count atomic.Int32
	// 发送事件
	for i := 0; i < 1; i++ {
		go func() {
			for k := 0; k < 1; k++ {
				for j := 0; j < 1; j++ {
					timeNow := time.Now().UnixNano()
					fmt.Println("--- v:", timeNow)
					ctx := metadata.Set(context.TODO(), "trace", "5fbd9fa17b6afccfb4bede12df68a111")
					ctx = context.WithValue(ctx, "trace", "5fbd9fa17b6afccfb4bede12df68a111")
					ctx = context.WithValue(ctx, "traceID", "5fbd9fa17b6afccfb4bede12df68a111")
					//GetEventBus(eventBusType).FireEvent(ctx, eventId, "test", &Student{Name: "test"}, "test")
					GetEventBus(eventBusType).FireEventWithTransaction(ctx, eventId, "test", &Student{Name: "test"}, "test", func(ctx context.Context, data interface{}) {
						fmt.Println("请求超时未处理")
					})
					//time.Sleep(time.Millisecond * 1)
					count.Add(1)
				}
				//time.Sleep(time.Second * 1)
			}

			fmt.Println(" 生产-------- count:", count.Load(), " 耗时：", time.Since(timeStart))
		}()
	}

	time.Sleep(time.Second * 1200)
}

// 这种方式可以初始化所有支持的事件总线类型，直接通过枚举类型区别调用即可，推荐使用这种方式
func TestEventBusMgrWithEnv(t *testing.T) {
	// 启动事件
	err := StartEventBusWithEnv(Dev, "test-server", []string{"test"})
	if err != nil {
		return
	}

	defer StopEventBus()

	// 订阅事件
	eventBusType := EnKafkaGroupBus
	var consumerCount atomic.Int32
	var consumerStart = time.Now()
	eventId := "event_user_login"
	GetEventBus(eventBusType).SubscribeEvent(eventId, "test", func(ctx context.Context, event, eventType string, data []byte, src string) error {
		//fmt.Println("eventId:", event, " data:", string(data), " src:", src)
		consumerCount.Add(1)
		fmt.Println(" 消费-------- count:", consumerCount.Load(), " 耗时：", time.Since(consumerStart))
		return nil
	})
	defer GetEventBus(eventBusType).UnsubscribeEvent(eventId, "test")

	timeStart := time.Now()
	var count atomic.Int32
	// 发送事件
	for i := 0; i < 10; i++ {
		go func() {
			for k := 0; k < 10; k++ {
				for j := 0; j < 10; j++ {
					timeNow := time.Now().UnixNano()
					fmt.Println("--- v:", timeNow)
					ctx := metadata.Set(context.TODO(), "key", "value")
					GetEventBus(eventBusType).FireEvent(ctx, eventId, "test", fmt.Sprintf("hello world %v", timeNow), "test")
					//time.Sleep(time.Millisecond * 1)
					count.Add(1)
				}
				//time.Sleep(time.Second * 1)
			}

			fmt.Println(" 生产-------- count:", count.Load(), " 耗时：", time.Since(timeStart))
		}()
	}

	time.Sleep(time.Second * 1200)
}

func Consumer() {
	// 订阅事件
	var consumerCount atomic.Int32
	var consumerStart = time.Now()
	eventId := "event_user_login"
	bus := NewEventBus()
	bus.SubscribeEvent(eventId, "test", func(ctx context.Context, event, eventType string, data []byte, src string) error {
		//fmt.Println("eventId:", event, " data:", string(data), " src:", src)
		consumerCount.Add(1)
		fmt.Println(" 消费2-------- count:", consumerCount.Load(), " 耗时：", time.Since(consumerStart), " value:", utiltools.ToJson(data))
		return nil
	})
	defer bus.UnsubscribeEvent(eventId, "test")

	// 指定kafka分组，同一个分组不存在重复消费问题，满足相应的业务场景需求
	err := bus.SetKafkaConnection([]string{"159.75.112.37:9092"}).SetKafkaGroup("event_bus_test22").StartEventBus("test_server", []string{"test"})
	if err != nil {
		panic(err)
	}

	defer bus.StopEventBus()

	time.Sleep(time.Second * 1200)
}

// kafka 一条消息同组只允许一个消费者消费
func TestOnceExecute(t *testing.T) {
	eventId := "event_user_login"
	// 订阅事件
	var consumerCount atomic.Int32
	var consumerStart = time.Now()

	EventBus.SubscribeEvent(eventId, "test", func(ctx context.Context, event, eventType string, data []byte, src string) error {
		//fmt.Println("eventId:", event, " data:", string(data), " src:", src)
		consumerCount.Add(1)
		fmt.Println(" 消费1-------- count:", consumerCount.Load(), " 耗时：", time.Since(consumerStart), " value:", utiltools.ToJson(data))
		return nil
	})
	defer EventBus.UnsubscribeEvent(eventId, "test")

	// 指定kafka分组，同一个分组不存在重复消费问题，满足相应的业务场景需求
	err := EventBus.SetKafkaConnection([]string{"159.75.112.37:9092"}).SetKafkaGroup("event_bus_test").StartEventBus("test_server", []string{"test"})
	if err != nil {
		panic(err)
	}

	defer EventBus.StopEventBus()

	// 测试
	/*go Consumer()
	time.Sleep(time.Second * 1200)*/

	timeStart := time.Now()
	var count atomic.Int32
	// 发送事件
	for i := 0; i < 1; i++ {
		go func() {
			for k := 0; k < 1; k++ {
				for j := 0; j < 10; j++ {
					ctx := metadata.Set(context.TODO(), "key", "value")
					EventBus.FireEvent(ctx, eventId, "test", fmt.Sprintf("hello world %v", utiltools.TimeFormat(time.Now())), "test")
					time.Sleep(time.Second * 1)
					count.Add(1)
				}
				//time.Sleep(time.Second * 1)
			}

			fmt.Println(" 生产-------- count:", count.Load(), " 耗时：", time.Since(timeStart))
		}()
	}

	time.Sleep(time.Second * 1200)
}

// 普通事件总线测试， 一条消息允许所有订阅的服务消费，因为每一个服务都分配了自己的分组，此类场景建议使用redis类型的
func TestEventBus(t *testing.T) {
	// 订阅事件
	var consumerCount atomic.Int32
	var consumerStart = time.Now()
	eventId := "event_user_login"
	EventBus.SubscribeEvent(eventId, "test", func(ctx context.Context, event, eventType string, data []byte, src string) error {
		//fmt.Println("eventId:", event, " data:", string(data), " src:", src)
		consumerCount.Add(1)
		fmt.Println(" 消费-------- count:", consumerCount.Load(), " 耗时：", time.Since(consumerStart))
		return nil
	})
	defer EventBus.UnsubscribeEvent(eventId, "test")

	// 启动事件总线
	/*	err := EventBus.SetKafkaConnection([]string{"159.75.112.37:9092"}).StartEventBus("test_server")
		if err != nil {
			panic(err)
		}

		defer EventBus.StopEventBus()*/

	// redis 事件总线 普通压测100线程，每个线程处理1000请求每秒处理10000多请求
	err := EventBus.SetRedisConnection(&RedisConfig{
		Addr:     "192.168.246.128:6379",
		Password: "",
		DB:       0,
	}).StartEventBus("test_server", []string{"test"})
	if err != nil {
		return
	}
	defer EventBus.StopEventBus()

	timeStart := time.Now()
	var count atomic.Int32
	// 发送事件
	for i := 0; i < 1; i++ {
		go func() {
			for k := 0; k < 1; k++ {
				for j := 0; j < 1; j++ {
					timeNow := time.Now().UnixNano()
					fmt.Println("--- v:", timeNow)
					ctx := metadata.Set(context.TODO(), "key", "value")
					EventBus.FireEvent(ctx, eventId, "test", fmt.Sprintf("hello world %v", timeNow), "test")
					//time.Sleep(time.Millisecond * 1)
					count.Add(1)
				}
				time.Sleep(time.Second * 1)
			}

			fmt.Println(" 生产-------- count:", count.Load(), " 耗时：", time.Since(timeStart))
		}()
	}

	time.Sleep(time.Second * 1200)
}
