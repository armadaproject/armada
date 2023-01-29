package pulsarutils

import (
	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/sirupsen/logrus"
)

type Scheduler int

const (
	Legacy Scheduler = iota
	Pulsar
)

const (
	SchedulerNameKey string = "schedulerName"
	PulsarScheduler  string = "pulsar"
	LegacyScheduler  string = "legacy"
)

func SchedulerFromMsg(msg pulsar.Message) Scheduler {
	s := msg.Properties()[SchedulerNameKey]
	switch s {
	case PulsarScheduler:
		return Pulsar
	case LegacyScheduler:
	case "":
		return Legacy
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
	}
	log.Warnf("Unknown scheduler [%d]. Defaulting to legacy scheduler", s)
	return LegacyScheduler
}

func ForPulsarScheduler(msg pulsar.Message) bool {
	return SchedulerFromMsg(msg) == Pulsar
}

func ForLegacyScheduler(msg pulsar.Message) bool {
	return SchedulerFromMsg(msg) == Legacy
}
