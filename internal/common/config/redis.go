package config

import (
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisConfig struct {
	// Either a single address or a seed list of host:port addresses
	Addrs           []string `validate:"required"`
	DB              int      `validate:"gte=0,lte=16"`
	Password        string
	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	PoolSize        int `validate:"required"`
	MinIdleConns    int
	MaxConnAge      time.Duration
	PoolTimeout     time.Duration
	IdleTimeout     time.Duration
	MasterName      string
}

func (rc RedisConfig) AsUniversalOptions() *redis.UniversalOptions {
	return &redis.UniversalOptions{
		Addrs:           rc.Addrs,
		DB:              rc.DB,
		Password:        rc.Password,
		MaxRetries:      rc.MaxRetries,
		MinRetryBackoff: rc.MaxRetryBackoff,
		MaxRetryBackoff: rc.MinRetryBackoff,
		DialTimeout:     rc.DialTimeout,
		ReadTimeout:     rc.ReadTimeout,
		WriteTimeout:    rc.WriteTimeout,
		PoolSize:        rc.PoolSize,
		MinIdleConns:    rc.MinIdleConns,
		ConnMaxLifetime: rc.MaxConnAge,
		PoolTimeout:     rc.PoolTimeout,
		ConnMaxIdleTime: rc.IdleTimeout,
		MasterName:      rc.MasterName,
	}
}
