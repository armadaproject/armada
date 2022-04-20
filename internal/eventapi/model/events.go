package model

type Event struct {
	Jobset   int64
	Sequence int64
	Payload  []byte
}

type EventUpdate struct {
	Jobset   int64
	Sequence int64
}
