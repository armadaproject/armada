package repository

import "time"

type Subscription struct {
	queue                string
	jobSet               string
	lastRequestTimeStamp int64
}

func NewSubscription(queue string, jobSet string) *Subscription {
	return &Subscription{queue: queue, jobSet: jobSet, lastRequestTimeStamp: time.Now().Unix()}
}
