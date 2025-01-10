package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"time"
)

type IRedisPublisher interface {
	// RegisterHandler 注册回调
	RegisterHandler(channelName string, callback HandlerFunc)
	// SubscriberPublisher 订阅推送
	SubscriberPublisher()
	// PublisherMessage 推送数据
	PublisherMessage(channelName string, uniqueIds []string, ops string, data interface{}, bProto bool) error
	// Publisher 推送消息
	Publisher(channelName string, uniqueIds []string, ops string, data []byte) error
}

type Message struct {
	ArrUniqueIds []string `json:"arr_unique_ids"`
	Ops          string   `json:"ops"`
	Data         []byte   `json:"data"`
}

// HandlerFunc 处理函数
type HandlerFunc func(uniqueIds []string, ops string, data []byte)

/**
 * RedisPublisher redis 推送功能
 * 基于Redis的发布订阅组件
 */
type RedisPublisher struct {
	client        *redis.Client
	subChannelMap map[string]HandlerFunc
}

func NewPubSub(client *redis.Client) IRedisPublisher {
	return &RedisPublisher{
		client:        client,
		subChannelMap: make(map[string]HandlerFunc),
	}
}

// RegisterHandler 注册回调
func (t *RedisPublisher) RegisterHandler(channelName string, callback HandlerFunc) {
	t.subChannelMap[channelName] = callback
}

// SubscriberPublisher 订阅发布
func (t *RedisPublisher) SubscriberPublisher() {
	channels := []string{}
	for k := range t.subChannelMap {
		channels = append(channels, k)
	}

	go t.subscriberPublish(channels, t.subChannelMap)
}

// PublisherMessage 推送消息
func (t *RedisPublisher) PublisherMessage(channelName string, uniqueIds []string, ops string, data interface{}, bProto bool) error {
	if data == nil {
		return fmt.Errorf("parameter error")
	}

	var _data []byte
	var err error
	if bProto {
		_data, err = TransProtoToJson(data.(proto.Message))
	} else {
		_data, err = json.Marshal(data)
	}

	if err != nil {
		return err
	}

	err = t.PushChannel(channelName, &Message{
		ArrUniqueIds: uniqueIds,
		Ops:          ops,
		Data:         _data,
	})

	return err
}

// Publisher 推送消息
func (t *RedisPublisher) Publisher(channelName string, uniqueIds []string, ops string, data []byte) error {
	err := t.PushChannel(channelName, &Message{
		ArrUniqueIds: uniqueIds,
		Ops:          ops,
		Data:         data,
	})

	return err
}

func (t *RedisPublisher) PushChannel(channelName string, msg interface{}) error {
	data, err := json.Marshal(msg)
	if err != nil {
		fmt.Println("push channelName:", channelName, "err:", err)
		return err
	}

	conn := t.client.Conn(context.Background())
	defer conn.Close()

	if value := conn.Publish(context.Background(), channelName, data); value.Err() != nil {
		fmt.Println("push channelName:", channelName, "err:", err)
		return err
	}

	return nil
}

func (t *RedisPublisher) subscriberPublish(channelPublisherNames []string, subChannelMap map[string]HandlerFunc) (err error) {
	fmt.Println("start publish Subscriber...")
lable:
	pubSub := t.client.Subscribe(context.Background(), channelPublisherNames...)
	if pubSub == nil {
		fmt.Println("failed to subscribe message,err:", err)
		time.Sleep(time.Second * 10)
		goto lable
	}
	defer pubSub.Close()

	for msg := range pubSub.Channel() {
		if subChannelMap == nil {
			continue
		}

		if h, bExist := subChannelMap[msg.Channel]; bExist && (h != nil) {
			msgRecv := &Message{}
			err = json.Unmarshal([]byte(msg.Payload), msgRecv)
			if err != nil {
				continue
			}

			h(msgRecv.ArrUniqueIds, msgRecv.Ops, msgRecv.Data)
		} else {
			fmt.Println("SubscriberPublish receive channel", msg.Channel)
		}
	}
	return
}

// TransProtoToJson proto to json
func TransProtoToJson(data proto.Message) (res []byte, err error) {
	marshal := protojson.MarshalOptions{
		AllowPartial:    false,
		UseProtoNames:   true,
		UseEnumNumbers:  true,
		EmitUnpopulated: true,
	}
	if res, err = marshal.Marshal(data); err != nil {
		return
	}
	return
}
