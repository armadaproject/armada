package pulsarutils

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
	SchedulerNameKey string = "schedulerName"
	PulsarScheduler  string = "pulsar"
	LegacyScheduler  string = "legacy"
	AllSchedulers    string = "all"
)

func SchedulerFromMsg(msg pulsar.Message) Scheduler {
	s := msg.Properties()[SchedulerNameKey]
	switch s {
	case PulsarScheduler:
		return Pulsar
	case LegacyScheduler:
	case "": // empty string means legacy scheduler for compatibility
		return Legacy
	case AllSchedulers:
		return All
	}
	log.Warnf("Unknown scheduler [%s] associated with pulsar message [%s]. Defaulting to legacy scheduler", s, msg.ID())
	return Legacy
}

func MsgPropertyFromScheduler(s Scheduler) string {
	switch s {
	case Pulsar:
		return PulsarScheduler
	case Legacy:
		return LegacyScheduler
	case All:
		return AllSchedulers
	}
	log.Warnf("Unknown scheduler [%d]. Defaulting to legacy scheduler", s)
	return LegacyScheduler
}

func ForPulsarScheduler(msg pulsar.Message) bool {
	s := SchedulerFromMsg(msg)
	return s == Pulsar || s == All
}

func ForLegacyScheduler(msg pulsar.Message) bool {
	s := SchedulerFromMsg(msg)
	return s == Legacy || s == All
}
