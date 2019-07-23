package configuration

import "time"

type ApplicationConfiguration struct {
	ClusterId           string
	InClusterDeployment bool
}

type TaskConfiguration struct {
	UtilisationReportingInterval           time.Duration
	ForgottenCompletedPodReportingInterval time.Duration
	JobLeaseRenewalInterval                time.Duration
	PodDeletionInterval                    time.Duration
	RequestNewJobsInterval                 time.Duration
}

type ArmadaConfiguration struct {
	Url string
}

type EventsConfiguration struct {
	Url                     string
	EventReportingInterval  time.Duration
	EventReportingBatchSize int
}

type Configuration struct {
	Application ApplicationConfiguration
	Task        TaskConfiguration
	Armada      ArmadaConfiguration
	Events      EventsConfiguration
}
