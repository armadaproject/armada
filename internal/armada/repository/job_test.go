package repository

import (
	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAddJob(t *testing.T) {
	// todo
	assert.True(t, true)
}

func TestZmove(t *testing.T) {
	db, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	r := redis.NewClient(&redis.Options{Addr: db.Addr()})
	r.ZAdd("set1", redis.Z{Member: "A", Score: 1})

	modified, _ := zmove(r, "set1", "set2", "A", 2).Int()
	assert.Equal(t, 1, modified)

	modified, _ = zmove(r, "set1", "set2", "A", 2).Int()
	assert.Equal(t, 0, modified)

	modified, _ = zmove(r, "set2", "set1", "A", 2).Int()
	assert.Equal(t, 1, modified)
}
