package event_bus

import (
	"context"
	"cp.wjaiot.net/cloud-server/smarlife/SaaSBP/go-common/goroutine_pool"
	libTrace "cp.wjaiot.net/cloud-server/smarlife/SaaSBP/go-common/lib/trace"
	"cp.wjaiot.net/cloud-server/smarlife/SaaSBP/go-common/utiltools"
	"encoding/json"
	"fmt"
	"go-micro.dev/v4/metadata"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	otelTrace "go.opentelemetry.io/otel/trace"
	"sync"
)

var (
	EventBus = NewEventBus()
)

const (
	EventBusTopic = "event.bus.topic"
)

type IEventBus interface {
	// StartEventBus 启动事件总线
	StartEventBus(serverName string, eventTypes []string) (err error)
	// StopEventBus 停止事件总线
	StopEventBus()
	// SetKafkaConnection 设置连接
	SetKafkaConnection(hosts []string) IEventBus
	// SetKafkaGroup 设置kafka分组，支持重复消费问题，同一个分组只会存在一个消费，如不指定会随机生成订阅者，每一个分组都会消费
	SetKafkaGroup(serverName string) IEventBus
	// SetRedisConnection 设置连接
	SetRedisConnection(conf *RedisConfig) IEventBus
	// SubscribeEvent 订阅事件
	SubscribeEvent(event, eventType string, handler HandlerFunc)
	// UnsubscribeEvent 退订事件
	UnsubscribeEvent(event, eventType string)
	// FireEvent 发射事件
	FireEvent(ctx context.Context, event, eventType string, data interface{}, src string) (err error)
}

// IPubSubClient 发布订阅客户端
type IPubSubClient interface {
	// Start 启动
	Start(serverName string, dispatch IDispatchSink) error
	// Stop 停止
	Stop() error
	// SetConnection 设置连接
	SetConnection(config interface{})
	// Publisher 发布数据
	Publisher(topic string, ops string, msg []byte) error
}

// IDispatchSink 消息派发
type IDispatchSink interface {
	// Dispatch 消息派发
	Dispatch(event string, data []byte)
	// GetEventBus 获取事件总线
	GetEventBus() IEventBus
	// RangeEventTyp 遍历事件类型
	RangeEventTyp(callback func(eventType string))
}

/**
 * EventBus 事件引擎
 * 这里支持两种组件实现事件总线，支持跨服务发送事件
 * 一种是基于kafka实现发送和消费 100万 消费4分钟 平均每秒4000多个请求，稳定，不怕并发量大 (1000 个协程for写入)
 * 一种是基于redis pub sub方式实现发送10万 消费10秒种，平均每秒1万请求和消费,不建议同时并发量大超过1000个线程for循环写入那种，容易丢数据 (100个协程for写入可以 1000个直接不动了，应该出问题了)
 * 一般建议使用redis的方式，比较轻量，并发量也不少，但是有可能丢数据，一般不会
 * 注意：这个是发送事件有两种模式：普通模式所有订阅的服务都会收到这个通知并触发相应的业务，指定kafka消费组模式和普通kafka一样使用，分组内服务交叉消费消息，如不得重复消费和使用的业务需要自己判断
 */
type eventBus struct {
	eventMap     sync.Map
	eventTypeMap sync.Map
	pubSubClient IPubSubClient
}

func NewEventBus() IEventBus {
	return &eventBus{
		eventMap:     sync.Map{},
		eventTypeMap: sync.Map{},
	}
}

// StartEventBus 启动事件总线
func (e *eventBus) StartEventBus(serverName string, eventTypes []string) (err error) {
	// 订阅的事件类型，主要用来开辟网络通道，区分通道提升并发能力以及隔离业务间的互相影响
	if len(eventTypes) > 0 {
		for i := 0; i < len(eventTypes); i++ {
			e.eventTypeMap.Store(eventTypes[i], struct{}{})
		}
	}

	// 启动发布订阅客户端
	err = e.pubSubClient.Start(serverName, e)
	if err != nil {
		return err
	}
	return
}

// StopEventBus 停止事件总线
func (e *eventBus) StopEventBus() {
	if e.pubSubClient != nil {
		err := e.pubSubClient.Stop()
		if err != nil {
			fmt.Println("eventBus err:", err)
		}
	}

	return
}

// SetEnv 设置环境
func (e *eventBus) SetEnv(env Env, typ PubTyp) IEventBus {
	switch typ {
	case RedisPubSub:
		if e.pubSubClient == nil {
			e.pubSubClient = newRedisClient()
		}

		e.pubSubClient.SetConnection(GetConnectByEnv(env, typ))
	case KafkaPubSub:
		if e.pubSubClient == nil {
			e.pubSubClient = newKafkaClient()
		}

		e.pubSubClient.SetConnection(GetConnectByEnv(env, typ))
	}

	return e
}

// SetKafkaConnection 设置连接
func (e *eventBus) SetKafkaConnection(hosts []string) IEventBus {
	if e.pubSubClient == nil {
		e.pubSubClient = newKafkaClient()
	}

	e.pubSubClient.SetConnection(&KafkaConf{
		Hosts:          hosts,
		IsNewestOffset: true,
	})
	return e
}

// SetKafkaGroup 设置kafka分组，支持重复消费问题，同一个分组只会存在一个消费，如不指定会随机生成订阅者，每一个分组都会消费
func (e *eventBus) SetKafkaGroup(serverName string) IEventBus {
	if e.pubSubClient == nil {
		e.pubSubClient = newKafkaClient()
	}
	e.pubSubClient.SetConnection(&KafkaConf{
		GroupId:        fmt.Sprintf("event_bus_%v", serverName),
		IsNewestOffset: false,
	})
	return e
}

// SetRedisConnection 设置连接
func (e *eventBus) SetRedisConnection(conf *RedisConfig) IEventBus {
	e.pubSubClient = newRedisClient()
	e.pubSubClient.SetConnection(conf)
	return e
}

// SubscribeEvent 订阅事件
func (e *eventBus) SubscribeEvent(event, eventType string, handler HandlerFunc) {
	if event == "" {
		fmt.Println("event is empty id:", event)
		return
	}

	if handler == nil {
		fmt.Println("handler is nil id:", event)
		return
	}

	e.eventMap.Store(fmt.Sprintf("%v_%v", eventType, event), handler)
	return
}

// UnsubscribeEvent 退订事件
func (e *eventBus) UnsubscribeEvent(event, eventType string) {
	if _, ok := e.eventMap.Load(fmt.Sprintf("%v_%v", eventType, event)); ok {
		e.eventMap.Delete(fmt.Sprintf("%v_%v", eventType, event))
	}
	return
}

// FireEvent 发射事件
func (e *eventBus) FireEvent(ctx context.Context, event, eventType string, data interface{}, src string) (err error) {
	md, _ := metadata.FromContext(ctx)
	m := metadata.Copy(md)
	libTrace.Inject(ctx, m)

	if v := ctx.Value("trace"); v != nil {
		m.Set("trace", v.(string))
	}

	if v := ctx.Value("traceID"); v != nil {
		m.Set("traceID", v.(string))
	}

	marshalData, err := json.Marshal(data)
	if err != nil {
		return
	}
	// 发送数据
	sendData := &Message{
		Event:     event,
		EventType: eventType,
		Src:       src,
		Body:      marshalData,
		Ctx:       m,
	}

	msg, err := json.Marshal(sendData)
	if err != nil {
		return err
	}
	return e.pubSubClient.Publisher(fmt.Sprintf("%v_%v", EventBusTopic, eventType), event, msg)
}

// Dispatch 派发事件
func (e *eventBus) Dispatch(event string, data []byte) {
	msg := &Message{
		Ctx: make(map[string]string),
	}
	err := json.Unmarshal(data, msg)
	if err != nil {
		fmt.Println("event bus Dispatch err:", err)
		return
	}

	var (
		ctx  = context.TODO()
		span otelTrace.Span
	)

	if v, ok := e.eventMap.Load(fmt.Sprintf("%v_%v", msg.EventType, msg.Event)); ok {
		goroutine_pool.GetPoolV3("event_bus", 50).Push(nil, func(data interface{}) error {
			traceId := ""
			var _ok bool
			// insight-home trace
			if traceId, _ok = msg.Ctx.Get("trace"); _ok {
				ctx = context.WithValue(ctx, "trace", traceId)
			}

			// xz-server trace
			if traceId, _ok = msg.Ctx.Get("traceID"); _ok {
				ctx = context.WithValue(ctx, "traceID", traceId)
			}

			bags, spanCtx := libTrace.Extract(ctx, msg.Ctx)
			isTraceIdValid := spanCtx.TraceID().String() != "00000000000000000000000000000000"
			// go-micro 链路追踪
			if isTraceIdValid {
				// 上游是go-micro 调用，下层可能是go-micro接收也可以是go-zero接收
				ctx = baggage.ContextWithBaggage(ctx, bags)
				ctx = otelTrace.ContextWithRemoteSpanContext(ctx, spanCtx)
			} else {
				// 上游是go-zero调用，下游可以是go-micro也可以是go-zero
				traceID, _ := otelTrace.TraceIDFromHex(traceId)
				spanId := traceId[15:31]
				spanID, _ := otelTrace.SpanIDFromHex(spanId)
				ctx = otelTrace.ContextWithRemoteSpanContext(
					ctx,
					otelTrace.NewSpanContext(otelTrace.SpanContextConfig{
						TraceID:    traceID,
						SpanID:     spanID,
						TraceFlags: otelTrace.FlagsSampled,
					}),
				)
			}

			// 存在链路追踪生成链路追踪信息
			if traceId != "" || isTraceIdValid {
				// 生成链路追踪信息，不管是go-micro还是go-zero
				tracer := otel.Tracer(
					"event_bus",
					otelTrace.WithInstrumentationVersion("1.0"),
				)
				attrs := []attribute.KeyValue{semconv.RPCServiceKey.String("mq")}
				spanName := fmt.Sprintf("%s.%s", msg.Event, msg.EventType)
				ctx, span = tracer.Start(
					ctx,
					spanName,
					otelTrace.WithSpanKind(otelTrace.SpanKindServer),
					otelTrace.WithAttributes(attrs...),
				)

				defer span.End()
				span.SetAttributes(
					attribute.String("mq.req", utiltools.ToJson(msg)),
					attribute.String("trace_id", spanCtx.TraceID().String()),
				)
			}

			// 回调处理函数
			err = v.(HandlerFunc)(ctx, msg.Event, msg.EventType, msg.Body, msg.Src)
			if traceId != "" || isTraceIdValid {
				if err != nil {
					fmt.Println("EventBus Dispatch err:", err)
					span.SetAttributes(
						// 设置事件为异常
						attribute.String("event", "error"),
						// 设置 message 为 err.Error().
						attribute.String("message", err.Error()),
					)
					span.SetStatus(codes.Error, err.Error())
				} else {
					// 如果没有发生异常，span 状态则为 ok
					span.SetStatus(codes.Ok, "OK")
				}
			}
			return nil
		})
	}
}

// GetEventBus 获取事件总线
func (e *eventBus) GetEventBus() IEventBus {
	return e
}

// RangeEventTyp 遍历事件类型
func (e *eventBus) RangeEventTyp(callback func(eventType string)) {
	e.eventTypeMap.Range(func(key, value any) bool {
		callback(key.(string))
		return true
	})
}
