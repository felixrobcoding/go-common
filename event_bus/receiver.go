package event_bus

import (
	"context"
	"cp.wjaiot.net/cloud-server/smarlife/SaaSBP/go-common/goroutine_pool"
	"cp.wjaiot.net/cloud-server/smarlife/SaaSBP/go-common/kafka"
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
	"time"
)

// ReceiverTest 接收器
type EventReceiver struct {
	dispatch IDispatchSink
}

func NewEventReceiver(dispatch IDispatchSink) kafka.Receiver {
	return &EventReceiver{
		dispatch: dispatch,
	}
}

// OnError when error happens, it will invoke OnError
func (r *EventReceiver) OnError(msg *sarama.ConsumerMessage) error {
	fmt.Println("OnError topic :", msg.Topic, " value:", string(msg.Value))
	goroutine_pool.GetPoolV2("event_bus").Push(nil, func(data interface{}) error {
		time.Sleep(time.Second * 5)
		topics := strings.Split(msg.Topic, "_")
		if len(topics) < 2 {
			return fmt.Errorf("topic illegal")
		}
		err := r.dispatch.GetEventBus().FireEvent(context.Background(), topics[0], topics[1], msg.Value, "OnError")
		if err != nil {
			fmt.Println("OnError topic :", msg.Topic, " value:", string(msg.Value), "eventBusContext failed")
		}
		return nil
	})
	return nil
}

// OnReceive if message receives, it will invoke OnReceive
func (r *EventReceiver) OnReceive(msg *sarama.ConsumerMessage) bool {
	r.dispatch.(*eventBus).Dispatch(msg.Topic, msg.Value)
	return true
}
