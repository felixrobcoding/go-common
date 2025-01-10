package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/felixrobcoding/go-common/utiltools"
	"github.com/go-redis/redis/v8"
	"github.com/rs/xid"
	"google.golang.org/protobuf/proto"
	"log"
	"time"
)

type StreamPubSub struct {
	client        *redis.Client
	group         string
	subChannelMap map[string]HandlerFunc
}

func (s StreamPubSub) RegisterHandler(channelName string, callback HandlerFunc) {
	s.subChannelMap[channelName] = callback
}

func (s StreamPubSub) SubscriberPublisher() {
	channels := []string{}
	for k := range s.subChannelMap {
		channels = append(channels, k)
	}

	go s.subscribe(context.Background(), channels, s.group, s.subChannelMap)
}

func (s StreamPubSub) PublisherMessage(channelName string, uniqueIds []string, ops string, data interface{}, bProto bool) error {
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

	return s.Publisher(channelName, uniqueIds, ops, _data)
}

func (s StreamPubSub) Publisher(channelName string, uniqueIds []string, ops string, data []byte) error {
	data, err := json.Marshal(&Message{
		ArrUniqueIds: uniqueIds,
		Ops:          ops,
		Data:         data,
	})
	if err != nil {
		log.Println("Publisher push channelName:", channelName, "err:", err)
		return err
	}

	err = s.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: channelName,
		MaxLen: 1000000,
		ID:     "",
		Values: map[string]interface{}{
			"msg": data,
		},
	}).Err()
	return err
}

func (s StreamPubSub) subscribe(ctx context.Context, channelPublisherNames []string, group string, subChannelMap map[string]HandlerFunc) (err error) {
	defer utiltools.ExceptionCatch()
	for i := 0; i < len(channelPublisherNames); i++ {
		err = s.client.XGroupCreate(ctx, channelPublisherNames[i], group, "0").Err()
		if err != nil {
			log.Println(err)
			if err = s.client.XGroupCreateMkStream(ctx, channelPublisherNames[i], group, "0").Err(); err != nil {
				log.Println(err)
			}
		}
	}

	for j := 0; j < len(channelPublisherNames); j++ {
		uniqueID := xid.New().String()
		go func(index int) {
			defer utiltools.ExceptionCatch()
			for {
				entries, _err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    group,
					Consumer: uniqueID,
					Streams:  []string{channelPublisherNames[index], ">"},
					Count:    20,
					Block:    0,
					NoAck:    false,
				}).Result()
				if _err != nil {
					log.Println("StreamPubSub subscribe err:", _err)
					time.Sleep(time.Second * 5)
					continue
				}

				for i := 0; i < len(entries[0].Messages); i++ {
					stream := entries[0].Stream
					messageID := entries[0].Messages[i].ID
					values := entries[0].Messages[i].Values

					if msg, ok := values["msg"]; ok {
						msgRecv := &Message{}
						err = json.Unmarshal([]byte(msg.(string)), msgRecv)
						if err != nil {
							continue
						}

						if h, _ok := subChannelMap[stream]; _ok {
							h(msgRecv.ArrUniqueIds, msgRecv.Ops, msgRecv.Data)
						}
					}

					s.client.XAck(ctx, stream, group, messageID)
				}
			}
		}(j)

	}
	return
}

func NewStreamPubSub(client *redis.Client, group string) IRedisPublisher {
	return &StreamPubSub{
		client:        client,
		group:         group,
		subChannelMap: make(map[string]HandlerFunc),
	}
}
