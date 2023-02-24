package repository

import "time"

type SubscribeTable struct {
	queue                string
	jobSet               string
	lastRequestTimeStamp int64
	err                  string
}

func NewSubscribeTable(queue string, jobSet string) *SubscribeTable {
	return &SubscribeTable{queue: queue, jobSet: jobSet, lastRequestTimeStamp: time.Now().Unix()}
}
