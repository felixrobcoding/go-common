package cache

import (
	"cp.wjaiot.net/cloud-server/smarlife/SaaSBP/go-common/redis"
	"encoding/json"
	"fmt"
	Redis "github.com/go-redis/redis/v8"
	gonanoid "github.com/matoous/go-nanoid"
	"github.com/patrickmn/go-cache"
	"regexp"
	"time"
)

var (
	GCache *cache.Cache
)

func init() {
	GCache = cache.New(30*time.Second, 5*time.Minute)
}

type ISyncCache interface {
	Start(serverName string) (err error)
	// Set 设置
	Set(k string, x interface{}, d time.Duration)
	// Get 获取
	Get(k string) (interface{}, bool)
	// Del 删除保持分布式同步
	Del(k string)
}

// SyncCache 分布式缓存
/**
 * 在缓存数据之前需要删除旧缓存，被动触发之后重新加载新数据加载到缓存种，当缓存数据变更的时候手动删除缓存
 * 注：这里只针对删除做了分布式同步，针对性能要求高的数据需要删除之后触发之后主动重新加入缓存
 */
type SyncCache struct {
	redisPublisher redis.IRedisPublisher
	uniqueId       string
	serverName     string
}

func NewSyncCache(client *Redis.Client) ISyncCache {
	uniqueId, _ := GenerateEntityID("")
	redisPublisher := redis.NewPubSub(client)
	return &SyncCache{
		redisPublisher: redisPublisher,
		uniqueId:       uniqueId,
	}
}

func (s *SyncCache) Start(serverName string) (err error) {
	s.serverName = serverName
	s.redisPublisher.RegisterHandler(serverName, func(uniqueIds []string, ops string, data []byte) {
		if ops == "cache" {
			var key string
			if err := json.Unmarshal(data, &key); err == nil && len(uniqueIds) > 0 && uniqueIds[0] != s.uniqueId {
				GCache.Delete(key)
			}
		}
	})
	s.redisPublisher.SubscriberPublisher()
	time.Sleep(time.Second * 1)
	return
}

// Set 设置
func (s *SyncCache) Set(k string, x interface{}, d time.Duration) {
	GCache.Set(k, x, d)
}

// Get 获取
func (s *SyncCache) Get(k string) (interface{}, bool) {
	return GCache.Get(k)
}

// Del 删除
func (s *SyncCache) Del(k string) {
	GCache.Delete(k)
	if err := s.redisPublisher.PublisherMessage(s.serverName, []string{s.uniqueId}, "cache", k, false); err != nil {
		fmt.Println("SyncCache PublisherMessage err:", err)
		return
	}
}

func GenerateEntityID(entityID string) (string, error) {
	if len(entityID) == 16 {
		if regexp.MustCompile(`^[a-z0-9A-Z]+$`).MatchString(entityID) {
			return entityID, nil
		}
	}
	return gonanoid.Generate("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890", 16)
}
