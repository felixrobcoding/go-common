package kafka

type KafkaConfig struct {
	// 一般配置
	Enabled     bool
	Connections []string

	// 消费协程数量，最少一个
	TaskGoroutineCount int
	// 消费分组Id
	GroupId string
	// 消费端topic
	ListenTopics []string

	// 鉴权
	SASLEnable bool
	SASLUser   string
	SASLPwd    string
}

type Config struct {
	Producer       KafkaConfig
	Consumer       KafkaConfig
	IsNewestOffset bool
}
