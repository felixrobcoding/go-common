package event_bus

import (
	"fmt"
	"sync"
)

/**
 * 事件总线管理器
 * 这里管理所有支持的事件总线，在使用的时候初始化所有需要的事件总线类型，然后直接获取使用即可
 */

var (
	busMap = sync.Map{}
)

type KafkaEventBusConfig struct {
	Hosts          []string
	IsNewestOffset bool
}

type EventBusConfig struct {
	NormalKafkaBus *KafkaEventBusConfig
	GroupKafkaBus  *KafkaEventBusConfig
	RedisBus       *RedisConfig
}

// StartEventBusWithEnv 通过环境变量启动
func StartEventBusWithEnv(env Env, serverName string, eventTypes []string) (err error) {
	kafkaConf := GetConnectByEnv(env, KafkaPubSub).(*KafkaConf)
	redisConf := GetConnectByEnv(env, RedisPubSub).(*RedisConfig)
	return StartEventBus(&EventBusConfig{
		NormalKafkaBus: &KafkaEventBusConfig{
			Hosts:          kafkaConf.Hosts,
			IsNewestOffset: kafkaConf.IsNewestOffset,
		},
		GroupKafkaBus: &KafkaEventBusConfig{
			Hosts:          kafkaConf.Hosts,
			IsNewestOffset: false,
		},
		RedisBus: redisConf,
	}, serverName, eventTypes)
}

// StartEventBus 初始化事件总线
func StartEventBus(conf *EventBusConfig, serverName string, eventTypes []string) (err error) {
	if conf == nil {
		return fmt.Errorf("config is emtpy")
	}

	// 初始化kafka通道事件总线
	if conf.NormalKafkaBus != nil {
		bus := GetEventBus(EnKafkaBus)
		err = bus.SetKafkaConnection(conf.NormalKafkaBus.Hosts).StartEventBus(serverName, eventTypes)
		if err != nil {
			panic(err)
		}
	}

	// 初始化指定group 通道事件总线
	if conf.GroupKafkaBus != nil {
		bus := GetEventBus(EnKafkaGroupBus)
		err = bus.SetKafkaConnection(conf.GroupKafkaBus.Hosts).SetKafkaGroup(serverName).StartEventBus(serverName, eventTypes)
		if err != nil {
			panic(err)
		}
	}

	// 初始化redis通道事件总线
	if conf.RedisBus != nil {
		bus := GetEventBus(EnRedisBus)
		err = bus.SetRedisConnection(conf.RedisBus).StartEventBus(serverName, eventTypes)
		if err != nil {
			panic(err)
		}
	}
	return
}

func StopEventBus() {
	busMap.Range(func(key, value any) bool {
		value.(IEventBus).StopEventBus()
		return true
	})
}

// GetEventBus 获取事件总线
func GetEventBus(busType EnEventBusType) IEventBus {
	if v, ok := busMap.Load(busType); ok {
		return v.(IEventBus)
	}

	bus := NewEventBus()
	busMap.Store(busType, bus)
	return bus
}

// GetRedisBus 获取redis bus
func GetRedisBus() IEventBus {
	return GetEventBus(EnRedisBus)
}

// GetKafkaBus 获取kafka bus
func GetKafkaBus() IEventBus {
	return GetEventBus(EnKafkaBus)
}

// GetKafkaGroupBus 获取kafka group bus
func GetKafkaGroupBus() IEventBus {
	return GetEventBus(EnKafkaGroupBus)
}
