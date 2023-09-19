package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"testing"
	"time"
)

func TestKafkaClient(t *testing.T) {
	topic := "test"
	// 构建配置
	connections := []string{"159.75.112.37:9092"}
	conf := Config{
		Producer: KafkaConfig{
			Enabled:     true,
			Connections: connections,
		},
		Consumer: KafkaConfig{
			Enabled:      true,
			Connections:  connections,
			GroupId:      "group_test",
			ListenTopics: []string{topic},
		},
	}

	k := NewKafkaClient(conf)
	// 监听消费
	err := k.Listen(map[string]Receiver{
		topic: new(ReceiverTest),
	})
	if err != nil {
		panic(err)
	}

	go func() {
		time.Sleep(time.Second * 10)
		// 发送消息
		err := k.SendMessage(topic, []byte("hello world"))
		if err != nil {
			fmt.Println("SendMessage err:", err)
			return
		}
	}()

	time.Sleep(time.Second * 30)
	return
}

// ReceiverTest 接收器
type ReceiverTest struct {
}

// when error happens, it will invoke OnError
func (r *ReceiverTest) OnError(msg *sarama.ConsumerMessage) error {
	fmt.Println("OnError topic :", msg.Topic, " value:", string(msg.Value))
	return nil
}

// if message receives, it will invoke OnReceive
func (r *ReceiverTest) OnReceive(msg *sarama.ConsumerMessage) bool {
	fmt.Println("OnReceive topic :", msg.Topic, " value:", string(msg.Value))
	return true
}
