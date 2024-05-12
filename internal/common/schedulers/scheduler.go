package schedulers

import (
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

// MsgPropertyFromScheduler returns the pulsar message property associated with the scheduler
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
