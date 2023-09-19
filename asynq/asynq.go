package asynq

import (
	"github.com/hibiken/asynq"
)

// Config 配置
type Config struct {
	Host string `json:"host"`
	Pwd  string `json:"pwd"`
	Db   int    `json:"db"`
}

type AsynqService struct {
	Consumer *asynq.Server
	Producer *asynq.Client
	conf     *Config
}

func NewAsynq(conf *Config) *AsynqService {
	serv := &AsynqService{
		conf: conf,
	}
	serv.Consumer = serv.newConsumer()
	serv.Producer = serv.newProducer()
	return serv
}

// newConsumer 初始化AsynqWorker
func (a *AsynqService) newConsumer() *asynq.Server {
	return asynq.NewServer(
		asynq.RedisClientOpt{
			Addr:     a.conf.Host,
			Password: a.conf.Pwd,
			DB:       a.conf.Db,
		},
		asynq.Config{
			Concurrency: 10,
			Queues: map[string]int{
				"critical": 6,
				"default":  3,
				"low":      1,
			},
		},
	)
}

// newProducer 初始化AsynqClient
func (a *AsynqService) newProducer() *asynq.Client {
	return asynq.NewClient(asynq.RedisClientOpt{
		Addr:     a.conf.Host,
		Password: a.conf.Pwd,
		DB:       a.conf.Db,
	})
}
