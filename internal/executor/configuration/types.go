package configuration

import "time"

type ApplicationConfiguration struct {
	ClusterId           string
	InClusterDeployment bool
}

type TaskConfiguration struct {
	UtilisationReportingInterval          time.Duration
	MissingJobEventReconciliationInterval time.Duration
	JobLeaseRenewalInterval               time.Duration
	AllocateSpareClusterCapacityInterval  time.Duration
}

type ArmadaConfiguration struct {
	Url string
}

type EventsConfiguration struct {
	Url string
}

type ExecutorConfiguration struct {
	Application ApplicationConfiguration
	Task        TaskConfiguration
	Armada      ArmadaConfiguration
	Events      EventsConfiguration
}
