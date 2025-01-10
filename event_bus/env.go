package event_bus

// Env 环境枚举
type Env string

const (
	Dev  = "dev"
	Test = "test"
	Prod = "prod"
)

// PubTyp 发布订阅组件类型
type PubTyp int

const (
	RedisPubSub PubTyp = iota + 1 // redis client pub sub
	KafkaPubSub                   // kafka client pub sub
)

// kafkaConnections kafka环境配置
var kafkaConnections = map[Env]*KafkaConf{
	Dev: &KafkaConf{
		Hosts:          []string{},
		GroupId:        "",
		IsNewestOffset: true,
	}, // 开发环境kafka
	Test: &KafkaConf{
		Hosts:          []string{},
		GroupId:        "",
		IsNewestOffset: true,
	}, // 测试环境kafka
	Prod: &KafkaConf{
		Hosts:          []string{},
		GroupId:        "",
		IsNewestOffset: true,
	}, // 正式环境kafka
}

// redisConnections redis环境配置
var redisConnections = map[Env]*RedisConfig{
	Dev: &RedisConfig{
		Addr:     "",
		Password: "",
		DB:       0,
	},
	Test: &RedisConfig{
		Addr:     "",
		Password: "",
		DB:       0,
	},
	Prod: &RedisConfig{
		Addr:     "",
		Password: "",
		DB:       0,
	},
}

// GetConnectByEnv 根据环境获取配置
func GetConnectByEnv(env Env, typ PubTyp) interface{} {
	switch typ {
	case KafkaPubSub:
		if v, ok := kafkaConnections[env]; ok {
			return v
		}
	case RedisPubSub:
		if v, ok := redisConnections[env]; ok {
			return v
		}
	}

	return nil
}
