package configuration

import "time"

type ApplicationConfiguration struct {
	ClusterId string
}

type KubernetesConfiguration struct {
	InClusterDeployment      bool
	KubernetesConfigLocation string
}

type AuthenticationConfiguration struct {
	EnableAuthentication bool
	Username             string
	Password             string
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
	Application    ApplicationConfiguration
	Authentication AuthenticationConfiguration
	Kubernetes     KubernetesConfiguration
	Task           TaskConfiguration
	Armada         ArmadaConfiguration
	Events         EventsConfiguration
}
