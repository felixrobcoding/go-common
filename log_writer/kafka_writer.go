package log_writer

import "cp.wjaiot.net/cloud-server/smarlife/SaaSBP/go-common/kafka"

// KafkaWriter 为 logger 提供写入 kafka 队列的 io 接口
type KafkaWriter struct {
	kafkaClient *kafka.KafkaClient
	topic       string
}

func NewKafkaWriter(hosts []string, topic string) *KafkaWriter {
	return &KafkaWriter{
		topic: topic,
		kafkaClient: kafka.NewKafkaClient(kafka.Config{
			Producer: kafka.KafkaConfig{
				Enabled:     true,
				Connections: hosts,
			},
			IsNewestOffset: false,
		}),
	}
}

func (k *KafkaWriter) Write(p []byte) (int, error) {
	if err := k.kafkaClient.SendMessage(k.topic, p); err != nil {
		return 0, err
	}
	return len(p), nil
}
