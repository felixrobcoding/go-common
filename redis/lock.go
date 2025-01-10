package redis

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"math/rand"
	"time"
)

var ErrNil = redis.Nil

/*
 * TRedLock 分布式锁
 * 自旋锁，加锁等待超时时间，在周期内会不断重复获取锁，否则返回失败
 */
type TRedLock struct {
	key   string
	value string
}

func (t *TRedLock) Lock(client *redis.Client, key string, timeout time.Duration) (bSuccess bool, err error) {
	t.key = fmt.Sprintf("__lock_%v__", key)
	t.value = fmt.Sprintf("__red_lock_%v__", rand.Intn(100000))

	for { // 针对问题1，使用循环
		ret := client.SetNX(context.Background(), t.key, t.value, timeout)
		err = ret.Err()
		if err != nil && err != ErrNil {
			continue
		}

		if err == ErrNil {
			err = nil
		}

		isLock := ret.Val()
		if isLock {
			bSuccess = true
			break
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
	return
}

func (t *TRedLock) UnLock(client *redis.Client) (bSuccess bool) {
	ret := client.Del(context.Background(), t.key)
	return ret.Val() > 0
}
