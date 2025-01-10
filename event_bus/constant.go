package event_bus

import (
	"cp.wjaiot.net/cloud-server/smarlife/SaaSBP/go-common/snowflake"
	"fmt"
	"time"
)

// EnEventBusType 事件总线类型
type EnEventBusType int

const (
	EnRedisBus      EnEventBusType = iota + 1 // redis 通道事件总线， 即时消费，启动后发现有消息时会即时消费掉，存在丢消息可能，一般不会
	EnKafkaBus                                //  kafka 通道事件总线，每次启动指定新的消费组，旧的消息直接不消费，会从最新的消息开始消费
	EnKafkaGroupBus                           //  指定消费者事件总线，适合类似kafka普通消费类型，只要是没有消费到的，下次启动会继续消费，同一个消费组不会重复消费，默认分组event_bus_serverName
)

// GetUniqueId 获取唯一标识ID
func GetUniqueId() string {
	s, _ := snowflake.NewSnowflake(int64(0), int64(0))
	orderNo := fmt.Sprintf("%s%d", time.Now().Format("20060102150405"), s.NextVal())
	return orderNo
}
