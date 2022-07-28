package repository

import "time"

type SubscribeTable struct {
	subscribedJobSet string
	lastRequestTimeStamp int64
}

func NewSubscribeTable(jobSetId string) *SubscribeTable {
	return &SubscribeTable{subscribedJobSet: jobSetId, lastRequestTimeStamp: time.Now().Unix()}
}
