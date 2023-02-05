package schedulers

import (
	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/sirupsen/logrus"
)

type Scheduler int

const (
	Legacy Scheduler = iota
	Pulsar
	All
)

const (
	PropertyName             string = "schedulerName"
	PulsarSchedulerAttribute string = "pulsar"
	LegacySchedulerAttribute string = "legacy"
	AllSchedulersAttribute   string = "all"
)

func SchedulerFromMsg(msg pulsar.Message) Scheduler {
	s := msg.Properties()[PropertyName]
	switch s {
	case PulsarSchedulerAttribute:
		return Pulsar
	case LegacySchedulerAttribute, "": // empty string means legacy scheduler for compatibility
		return Legacy
	case AllSchedulersAttribute:
		return All
	}
	log.Warnf("Unknown scheduler [%s] associated with pulsar message [%s]. Defaulting to legacy scheduler", s, msg.ID())
	return Legacy
}

func MsgPropertyFromScheduler(s Scheduler) string {
	switch s {
	case Pulsar:
		return PulsarSchedulerAttribute
	case Legacy:
		return LegacySchedulerAttribute
	case All:
		return AllSchedulersAttribute
	}
	log.Warnf("Unknown scheduler [%d]. Defaulting to legacy scheduler", s)
	return LegacySchedulerAttribute
}

func ForPulsarScheduler(msg pulsar.Message) bool {
	s := SchedulerFromMsg(msg)
	return s == Pulsar || s == All
}

func ForLegacyScheduler(msg pulsar.Message) bool {
	s := SchedulerFromMsg(msg)
	return s == Legacy || s == All
}
