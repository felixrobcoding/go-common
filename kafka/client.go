package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
)

var (
	taskGoroutineCount int
	wg                 = &sync.WaitGroup{}
	ctx, cancel        = context.WithCancel(context.Background())
)

type KafkaClient struct {
	consumer sarama.ConsumerGroup
	producer sarama.SyncProducer
	conf     Config
}

func NewKafkaClient(conf Config) *KafkaClient {
	k := &KafkaClient{
		conf: conf,
	}
	// 消费的协程数量
	taskGoroutineCount = conf.Consumer.TaskGoroutineCount
	k.producer = k.newProducer()
	k.consumer = k.newConsumer(conf.IsNewestOffset)
	return k
}

func (kafkaClient *KafkaClient) newProducer() sarama.SyncProducer {
	// 初始化服务端
	if kafkaClient.conf.Producer.Enabled {

		config := sarama.NewConfig()
		config.Producer.RequiredAcks = sarama.WaitForAll // 发送完数据需要leader和follow都确认
		config.Producer.Return.Successes = true
		config.Net.SASL.Enable = kafkaClient.conf.Producer.SASLEnable
		config.Net.SASL.User = kafkaClient.conf.Producer.SASLUser
		config.Net.SASL.Password = kafkaClient.conf.Producer.SASLPwd

		hosts := kafkaClient.conf.Producer.Connections
		producer, err := sarama.NewSyncProducer(hosts, config)
		if err != nil {
			fmt.Println(fmt.Sprintf("init kafka producer client new consumer group object error:%v", err))
		}

		kafkaClient.producer = producer
	}
	return kafkaClient.producer
}

func (kafkaClient *KafkaClient) newConsumer(isNewestOffset bool) sarama.ConsumerGroup {
	offset := sarama.OffsetOldest
	if isNewestOffset {
		offset = sarama.OffsetNewest
	}
	// 初始化消费端
	if kafkaClient.conf.Consumer.Enabled {
		config := sarama.NewConfig()
		config.Version = sarama.V2_2_0_0
		config.Consumer.Return.Errors = true
		config.Consumer.Offsets.AutoCommit.Enable = true
		config.Consumer.Offsets.Initial = offset
		config.Net.SASL.Enable = kafkaClient.conf.Consumer.SASLEnable
		config.Net.SASL.User = kafkaClient.conf.Consumer.SASLUser
		config.Net.SASL.Password = kafkaClient.conf.Consumer.SASLPwd

		hosts := kafkaClient.conf.Consumer.Connections
		consumer, err := sarama.NewConsumerGroup(hosts, kafkaClient.conf.Consumer.GroupId, config)
		if err != nil {
			fmt.Println(fmt.Sprintf("init kafka consumer client new consumer group object error:%v", err))
		}

		kafkaClient.consumer = consumer
	}
	return kafkaClient.consumer
}

func (kafkaClient KafkaClient) Listen(topicReceiver map[string]Receiver) (err error) {

	fmt.Println("kafka listen...")

	topics := kafkaClient.conf.Consumer.ListenTopics

	// 监听事件
	receivers := ConsumeReceiver{topicReceiver: topicReceiver}
	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
		}()
		for {
			// 监听kafka
			if err = kafkaClient.consumer.Consume(ctx, topics, receivers); err != nil {
				fmt.Println(fmt.Sprintf("Error from consumer: %v", err))
			}

			// 检查上下文是否被取消，表示消费者应该停止
			if ctx.Err() != nil {
				fmt.Println(fmt.Sprintln(ctx.Err()))
				return
			}
		}
	}()

	fmt.Println("Sarama consumer up and running!...")
	return
}

func (kafkaClient *KafkaClient) SendMessage(topic string, message []byte) (err error) {
	if kafkaClient.conf.Producer.Enabled {
		// 发送消息
		pid, offset, err := kafkaClient.producer.SendMessage(
			&sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(message),
			})
		if err != nil {
			return err
		}
		fmt.Println(fmt.Sprintf("kafka producer send message pid:%v, offset:%v, message:%v", pid, offset, string(message)))
	}
	return
}

func (kafkaClient *KafkaClient) SendMessageWithKey(topic string, key, message []byte) (err error) {
	if kafkaClient.conf.Producer.Enabled {
		// 发送消息
		pid, offset, err := kafkaClient.producer.SendMessage(
			&sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.ByteEncoder(key),
				Value: sarama.ByteEncoder(message),
			})
		if err != nil {
			return err
		}
		fmt.Println(fmt.Sprintf("kafka producer send message pid:%v, offset:%v, message:%v", pid, offset, string(message)))
	}
	return
}

func (kafkaClient *KafkaClient) Close() {
	if kafkaClient.conf.Consumer.Enabled {
		cancel()
		wg.Wait()
		err := kafkaClient.consumer.Close()
		if err != nil {
			fmt.Println(fmt.Sprintf("kafka consumer close error:%v", err))
		}
	}
	if kafkaClient.conf.Producer.Enabled {
		err := kafkaClient.producer.Close()
		if err != nil {
			fmt.Println(fmt.Sprintf("kafka producer close error:%v", err))
		}
	}
	return
}
