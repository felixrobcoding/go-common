package cache

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:       "192.168.246.128:6379",
		Password:   "",
		DB:         0,
		PoolSize:   30,
		MaxRetries: 5,
	})
	cache := NewSyncCache(client)
	cache.Start("test")
	go func() {
		for {
			fmt.Println(cache.Get("test_key"))
			time.Sleep(time.Second * 2)
		}
	}()

	cache.Set("test_key", 1, time.Second*30)

	cache.Del("test_key")

	time.Sleep(time.Second * 60)
	return
}
