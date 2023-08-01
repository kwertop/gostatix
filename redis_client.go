package gostatix

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var once sync.Once
var redisClient *redis.Client

type RedisConnOptions struct {
	DB                int
	Network           string
	Address           string
	Username          string
	Password          string
	ConnectionTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	PoolSize          int
	TLSConfig         *tls.Config
}

func GetRedisClient() *redis.Client {
	return redisClient
}

func MakeRedisClient(options RedisConnOptions) {
	once.Do(func() {
		redisClient = redis.NewClient(&redis.Options{
			DB:           options.DB,
			Network:      options.Network,
			Addr:         options.Address,
			Username:     options.Username,
			Password:     options.Password,
			DialTimeout:  options.ConnectionTimeout,
			ReadTimeout:  options.ReadTimeout,
			WriteTimeout: options.WriteTimeout,
			PoolSize:     options.PoolSize,
			TLSConfig:    options.TLSConfig,
		})
	})
}

func ParseRedisURI(uri string) (*RedisConnOptions, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("gostatix: could not parse redis uri: %v", err)
	}
	if u.Scheme == "redis" || u.Scheme == "rediss" {
		options, err := redis.ParseURL(uri)
		if err != nil {
			return nil, fmt.Errorf("gostatix: error while parsing redis uri: %v", err)
		}
		redisConnOptions := makeConnOptions(options)
		return redisConnOptions, nil
	} else {
		return nil, fmt.Errorf("gostatix: unsupported uri scheme")
	}
}

func makeConnOptions(options *redis.Options) *RedisConnOptions {
	var opt *RedisConnOptions
	opt.DB = options.DB
	opt.Network = options.Network
	opt.Address = options.Addr
	opt.Username = options.Username
	opt.Password = options.Password
	opt.ConnectionTimeout = options.DialTimeout
	opt.ReadTimeout = options.ReadTimeout
	opt.WriteTimeout = options.WriteTimeout
	opt.PoolSize = options.PoolSize
	opt.TLSConfig = options.TLSConfig
	return opt
}
