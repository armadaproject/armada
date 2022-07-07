package repository

import (
	"testing"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"

)
func TestHealthCheck(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6380", Password: "", DB: 1})
	defer client.FlushDB()
	defer client.Close()
	err := HealthCheck(client)
	assert.Nil(t, err)
}
