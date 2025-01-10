package event_bus

import (
	"context"
	"sync"
	"time"
)

var TimeOutComponent = NewTimeOutComponent()

// DataContext 数据现场
type DataContext struct {
	Timer *time.Timer `json:"timer"`
}

// TimeOutComponent 超时组件
type timeOutComponent struct {
	timeOutMap sync.Map
}

func NewTimeOutComponent() *timeOutComponent {
	return &timeOutComponent{
		timeOutMap: sync.Map{},
	}
}

// AddTimeOutHandler 添加超时处理器
func (t *timeOutComponent) AddTimeOutHandler(ctx context.Context, uniqueId string, data interface{}, duration time.Duration, timeOutCall func(ctx context.Context, data interface{})) {
	t.timeOutMap.Store(uniqueId, &DataContext{
		Timer: time.AfterFunc(duration, func() {
			t.DelTimeOutHandler(uniqueId)
			timeOutCall(ctx, data)
		}),
	})
}

// DelTimeOutHandler 删除处理器
func (t *timeOutComponent) DelTimeOutHandler(uniqueId string) bool {
	if v, ok := t.timeOutMap.Load(uniqueId); ok {
		v.(*DataContext).Timer.Stop()
		t.timeOutMap.Delete(uniqueId)
		return true
	}
	return false
}
