package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
)

type Receiver interface {
	OnError(msg *sarama.ConsumerMessage) error  // when error happens, it will invoke OnError
	OnReceive(msg *sarama.ConsumerMessage) bool // if message receives, it will invoke OnReceive
}

type ConsumeReceiver struct {
	topicReceiver map[string]Receiver
}

func (consume ConsumeReceiver) Setup(_ sarama.ConsumerGroupSession) error { return nil }

func (consume ConsumeReceiver) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (consume ConsumeReceiver) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {

	// 最大任务
	taskMax := 1
	if taskGoroutineCount > 0 {
		taskMax = taskGoroutineCount
	}

	taskChan := make(chan *sarama.ConsumerMessage, taskMax)

	// 初始化任务
	taskFunc := func(messages <-chan *sarama.ConsumerMessage) {
		for message := range messages {
			handler := consume.topicReceiver[message.Topic]
			if !handler.OnReceive(message) {
				if err := handler.OnError(message); err != nil {
					fmt.Println(fmt.Sprintf("Listen kafka on error message:%+v, error:%v", message, err))
				}
			}
			session.MarkMessage(message, "")
		}
	}

	// 初始化协程
	for i := 0; i < taskMax; i++ {
		go taskFunc(taskChan)
	}

	// 执行任务
	for message := range claim.Messages() {
		taskChan <- message
	}
	return nil
}
