package repository

type JobSetKey struct {
	Queue    string
	JobSetId string
}

type JobSetSubscriptionInfo struct {
	JobSetKey

	FromMessageId string
}
