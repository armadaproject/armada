package eventapi

type Event struct {
	Jobset   int64
	Sequence int64
	Payload  []byte
}

type EventSubscription struct {
	SubscriptionId int64
	Channel        chan []*Event
}

type JobSet struct {
	Id     int64
	Queue  string
	Jobset string
}
