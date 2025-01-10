package event_bus

import (
	"context"
	"fmt"
	"github.com/felixrobcoding/go-common/redis"
	Redis "github.com/go-redis/redis/v8"
	"time"
)

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type redisClient struct {
	redisPubSub redis.IRedisPublisher
	conf        *RedisConfig
	dispatch    IDispatchSink
	UniqueId    string
}

func newRedisClient() IPubSubClient {
	return &redisClient{}
}

// Start 启动
func (c *redisClient) Start(serverName string, dispatch IDispatchSink) (err error) {
	if c.conf == nil {
		panic("event_bus start redis client error ")
	}

	c.UniqueId = c.genGroupId(serverName)
	c.dispatch = dispatch

	client := Redis.NewClient(&Redis.Options{
		Addr:       c.conf.Addr,
		Password:   c.conf.Password,
		DB:         c.conf.DB,
		PoolSize:   300,
		MaxRetries: 5,
	})

	if err = client.Ping(context.Background()).Err(); err != nil {
		panic(err)
	}

	c.redisPubSub = redis.NewStreamPubSub(client, fmt.Sprintf("group_event_bus_%v", serverName))

	// 监听消费
	c.dispatch.RangeEventTyp(func(eventType string) {
		c.redisPubSub.RegisterHandler(fmt.Sprintf("%v_%v", EventBusTopic, eventType), func(uniqueIds []string, ops string, data []byte) {
			/*		if len(uniqueIds) > 0 && uniqueIds[0] == c.UniqueId {
						return
					}
			*/
			if c.dispatch != nil {
				c.dispatch.Dispatch(ops, data)
			}
		})
	})

	c.redisPubSub.SubscriberPublisher()

	time.Sleep(time.Second * 1)
	return nil
}

// Stop 停止
func (c *redisClient) Stop() error {
	return nil
}

// SetConnection 设置连接
func (c *redisClient) SetConnection(config interface{}) {
	c.conf = config.(*RedisConfig)
	return
}

// Publisher 发布数据
func (c *redisClient) Publisher(topic string, ops string, msg []byte) error {
	return c.redisPubSub.Publisher(topic, []string{c.UniqueId}, ops, msg)
}

func (c *redisClient) genGroupId(serverName string) string {
	return fmt.Sprintf("event_bus_%v_%v", serverName, time.Now().UnixNano())
}
