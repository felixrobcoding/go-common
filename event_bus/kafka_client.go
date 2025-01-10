package event_bus

import (
	"fmt"
	"github.com/felixrobcoding/go-common/kafka"
	"time"
)

type KafkaConf struct {
	Hosts          []string
	GroupId        string
	IsNewestOffset bool
}

type kafkaClient struct {
	kafkaClient *kafka.KafkaClient
	conf        *KafkaConf
}

func newKafkaClient() IPubSubClient {
	return &kafkaClient{}
}

// Start 启动
func (c *kafkaClient) Start(serverName string, dispatch IDispatchSink) (err error) {
	// 构建订阅列表
	var topics []string
	var listenMap = make(map[string]kafka.Receiver)
	receiver := NewEventReceiver(dispatch)
	dispatch.RangeEventTyp(func(eventType string) {
		topic := fmt.Sprintf("%v_%v", EventBusTopic, eventType)
		if _, ok := listenMap[topic]; ok {
			return
		}

		listenMap[topic] = receiver
		topics = append(topics, topic)
	})

	c.kafkaClient = kafka.NewKafkaClient(kafka.Config{
		Producer: kafka.KafkaConfig{
			Enabled:     true,
			Connections: c.conf.Hosts,
		},
		Consumer: kafka.KafkaConfig{
			Enabled:      true,
			Connections:  c.conf.Hosts,
			GroupId:      c.genGroupId(serverName),
			ListenTopics: topics,
		},
		IsNewestOffset: true,
	})

	// 监听消费
	err = c.kafkaClient.Listen(listenMap)

	time.Sleep(time.Second * 1)
	return nil
}

// Stop 停止
func (c *kafkaClient) Stop() error {
	if c.kafkaClient != nil {
		c.kafkaClient.Close()
	}
	return nil
}

// SetConnection 设置连接
func (c *kafkaClient) SetConnection(config interface{}) {
	if c.conf == nil {
		c.conf = &KafkaConf{}
	}

	// 设置host地址
	conf := config.(*KafkaConf)
	if conf != nil && len(conf.Hosts) > 0 {
		c.conf.Hosts = conf.Hosts
	}

	// 设置groupId
	if conf != nil && conf.GroupId != "" {
		c.conf.GroupId = conf.GroupId
	}

	// 设置isNewOffset
	if conf != nil {
		c.conf.IsNewestOffset = conf.IsNewestOffset
	}

	return
}

// Publisher 发布数据
func (c *kafkaClient) Publisher(topic string, ops string, msg []byte) error {
	return c.kafkaClient.SendMessage(topic, msg)
}

func (c *kafkaClient) genGroupId(serverName string) string {
	if c.conf.GroupId != "" {
		return c.conf.GroupId
	}
	return fmt.Sprintf("event_bus_%v_%v", serverName, time.Now().UnixNano())
}
