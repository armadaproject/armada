package configuration

import (
	"time"

	"github.com/G-Research/armada/internal/common/oidc"
)

type ApplicationConfiguration struct {
	ClusterId string
}

type KubernetesConfiguration struct {
	InClusterDeployment      bool
	KubernetesConfigLocation string
}

type BasicAuthenticationConfiguration struct {
	EnableAuthentication bool
	Username             string
	Password             string
}

type TaskConfiguration struct {
	UtilisationReportingInterval          time.Duration
	MissingJobEventReconciliationInterval time.Duration
	JobLeaseRenewalInterval               time.Duration
	AllocateSpareClusterCapacityInterval  time.Duration
	StuckPodScanInterval                  time.Duration
	PodDeletionInterval                   time.Duration
}

type ArmadaConfiguration struct {
	Url string
}

type ExecutorConfiguration struct {
	MetricsPort                 uint16
	Application                 ApplicationConfiguration
	BasicAuth                   BasicAuthenticationConfiguration
	OpenIdPasswordAuth          oidc.ClientPasswordDetails
	OpenIdClientCredentialsAuth oidc.ClientCredentialsDetails
	Kubernetes                  KubernetesConfiguration
	Task                        TaskConfiguration
	Armada                      ArmadaConfiguration
}
