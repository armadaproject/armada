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

type Configuration struct {
	Application ApplicationConfiguration
	Task        TaskConfiguration
}
