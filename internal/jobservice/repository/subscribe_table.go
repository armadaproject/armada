package repository

import "time"

type SubscribeTable struct {
	subscribedJobSet     string
	lastRequestTimeStamp int64
}

func NewSubscribeTable(queueJobSet string) *SubscribeTable {
	return &SubscribeTable{subscribedJobSet: queueJobSet, lastRequestTimeStamp: time.Now().Unix()}
}
