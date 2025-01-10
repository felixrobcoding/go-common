package asynq

import (
	"context"
	"encoding/json"
	"github.com/hibiken/asynq"
	"time"
)

// Config 配置
type Config struct {
	Host string `json:"host"`
	Pwd  string `json:"pwd"`
	Db   int    `json:"db"`
}

// AsynqService 延迟队列
type AsynqService struct {
	Consumer *asynq.Server
	Producer *asynq.Client
	ServeMux *asynq.ServeMux
	conf     *Config
}

func NewAsynq(conf *Config) *AsynqService {
	serv := &AsynqService{
		conf: conf,
	}
	serv.Consumer = serv.newConsumer()
	serv.Producer = serv.newProducer()
	serv.ServeMux = asynq.NewServeMux()
	return serv
}

func (a *AsynqService) Start() error {
	if err := a.Consumer.Start(a.ServeMux); err != nil {
		return err
	}
	return nil
}

// SubscribeConsumer 订阅
func (a *AsynqService) SubscribeConsumer(topic string, handler func(context.Context, *asynq.Task) error) {
	a.ServeMux.HandleFunc(topic, handler)
	return
}

// Publisher 发送消息
func (a *AsynqService) Publisher(topic string, data interface{}, delayDuration time.Duration) error {
	sendData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	taskMsg := asynq.NewTask(topic, sendData)
	// 延迟发送
	option := asynq.ProcessIn(delayDuration)
	_, err = a.Producer.Enqueue(taskMsg, option)
	if err != nil {
		return err
	}

	return nil
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
