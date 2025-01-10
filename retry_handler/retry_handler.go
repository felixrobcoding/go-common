package retry_handler

import (
	"context"
	Asynq "cp.wjaiot.net/cloud-server/smarlife/SaaSBP/go-common/asynq"
	"encoding/json"
	"fmt"
	"github.com/hibiken/asynq"
	"sync"
	"time"
)

// RetryHandlerConfig 重试配置
type RetryHandlerConfig struct {
	Topic                string        `json:"topic"`                   // 必填：重试主题
	MinRetryTimeInterval time.Duration `json:"min_retry_time_interval"` // 必填：最小重试间隔 如5秒 往后每一次执行都乘以2倍时长
	MaxRetryTimeInterval time.Duration `json:"max_retry_time_interval"` // 必填：最大重试间隔 如1个小时
	TotalRetryTime       time.Duration `json:"total_retry_time"`        // 必填：最大重试时间 如 1天
	MaxRetryCount        int           `json:"max_retry_count"`         // 选填：最大重试次数 小于等于0 不起作用
}

// TaskData 任务数据
type TaskData struct {
	// 回调现场数据
	Context []byte `json:"context"`
	// 重试次数
	RetryCount int `json:"retry_count"`
	// 开始重试时间
	FirstRetryTime int64 `json:"first_retry_time"`
	// 主题
	Topic string `json:"topic"`
}

type IRetryHandler interface {
	// Subscribe 订阅
	// callback 重试执行回调
	// alarmCall 超过最大次数，超过时间失败告警回调
	Subscribe(ctx context.Context, conf *RetryHandlerConfig, callback func(ctx context.Context, data []byte) error,
		alarmCall func(ctx context.Context, data []byte)) (err error)
	// Publisher 发送消息
	Publisher(ctx context.Context, topic string, data interface{}) error
	// Start 启动
	Start(conf *Asynq.Config) error
}

type RetryHandler struct {
	handlerMap sync.Map
	configMap  sync.Map
	alarmMap   sync.Map
	serv       *Asynq.AsynqService
}

func NewRetryHandler() IRetryHandler {
	return &RetryHandler{
		handlerMap: sync.Map{},
		alarmMap:   sync.Map{},
		configMap:  sync.Map{},
	}
}

func (r *RetryHandler) Start(conf *Asynq.Config) error {
	r.serv = Asynq.NewAsynq(conf)
	r.handlerMap.Range(func(key, value any) bool {
		if v, ok := r.configMap.Load(key); ok {
			r.serv.SubscribeConsumer(v.(*RetryHandlerConfig).Topic, r.handler)
		}

		return true
	})

	return r.serv.Start()
}

// Subscribe 订阅
func (r *RetryHandler) Subscribe(ctx context.Context, conf *RetryHandlerConfig, callback func(ctx context.Context, data []byte) error,
	alarmCall func(ctx context.Context, data []byte)) (err error) {
	if conf == nil || conf.Topic == "" || conf.MaxRetryTimeInterval <= 0 || conf.MinRetryTimeInterval <= 0 || conf.TotalRetryTime <= 0 ||
		conf.MinRetryTimeInterval > conf.MaxRetryTimeInterval || conf.TotalRetryTime <= conf.MinRetryTimeInterval {
		return fmt.Errorf("conf illlegal")
	}

	if callback == nil {
		return fmt.Errorf("callback is nil")
	}

	if callback != nil {
		r.handlerMap.Store(conf.Topic, callback)
	}

	if alarmCall != nil {
		r.alarmMap.Store(conf.Topic, alarmCall)
	}

	r.configMap.Store(conf.Topic, conf)
	return nil
}

// Publisher 发送消息
func (r *RetryHandler) Publisher(ctx context.Context, topic string, data interface{}) error {
	sendData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// 投递数据
	return r.publisher(ctx, &TaskData{
		Context: sendData,
		Topic:   topic,
	})
}

// publisher 推送数据
func (r *RetryHandler) publisher(ctx context.Context, data *TaskData) (err error) {
	if r.serv == nil {
		return fmt.Errorf("asynq start failed")
	}

	if data == nil {
		return fmt.Errorf("data is nil")
	}

	// 获取配置
	conf, ok := r.configMap.Load(data.Topic)
	if !ok || conf == nil {
		return fmt.Errorf("load conf failed")
	}

	data.RetryCount += 1
	if data.FirstRetryTime == 0 {
		data.FirstRetryTime = time.Now().Unix()
	}

	// 最大次数判断
	maxRetryCount := conf.(*RetryHandlerConfig).MaxRetryCount
	if maxRetryCount > 0 && data.RetryCount > maxRetryCount {
		if v, _ok := r.alarmMap.Load(data.Topic); _ok {
			v.(func(ctx context.Context, data []byte))(ctx, data.Context)
		}
		return
	}

	// 计算间隔时间
	times := 1
	for i := 1; i < data.RetryCount; i++ {
		times = times * 2
	}

	// 重试时间
	retryDuration := conf.(*RetryHandlerConfig).MinRetryTimeInterval * time.Duration(times)
	// 超过单次重试时间采用最大值，为一个小时
	if retryDuration > conf.(*RetryHandlerConfig).MaxRetryTimeInterval {
		retryDuration = conf.(*RetryHandlerConfig).MaxRetryTimeInterval
	}
	// 总重试次数超过总共重试时长直接丢去，告警
	if time.Second*time.Duration(time.Now().Unix()-data.FirstRetryTime) > conf.(*RetryHandlerConfig).TotalRetryTime {
		if v, _ok := r.alarmMap.Load(data.Topic); _ok {
			v.(func(ctx context.Context, data []byte))(ctx, data.Context)
		}
		return nil
	}

	// 投递数据
	return r.serv.Publisher(data.Topic, data, retryDuration)
}

// handler 回调处理器
func (r *RetryHandler) handler(ctx context.Context, task *asynq.Task) error {
	recvData := &TaskData{}
	err := json.Unmarshal(task.Payload(), recvData)
	if err != nil {
		return nil
	}

	// 加载回调函数
	h, ok := r.handlerMap.Load(recvData.Topic)
	if !ok {
		return nil
	}

	// 进行回调
	err = h.(func(ctx context.Context, data []byte) error)(ctx, recvData.Context)
	if err == nil {
		return nil
	}

	// 投递数据
	return r.publisher(ctx, recvData)
}
