package event_bus

import (
	"context"
	"go-micro.dev/v4/metadata"
)

type Message struct {
	Ctx       metadata.Metadata `json:"ctx"`
	Event     string            `json:"event"`
	EventType string            `json:"event_type"`
	Src       string            `json:"src"`
	Body      []byte            `json:"body"`
	UniqueId  string            `json:"unique_id"`
}

// HandlerFunc 回调处理函数类型
type HandlerFunc func(ctx context.Context, event, eventType string, data []byte, src string) error
