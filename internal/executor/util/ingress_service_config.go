package util

import (
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/pkg/api"
)

type IngressServiceType int

const (
	Ingress IngressServiceType = iota
	NodePort
	Headless
)

func (st IngressServiceType) String() string {
	return []string{"Ingress", "NodePort", "Headless"}[st]
}

type IngressServiceConfig struct {
	Type         IngressServiceType
	Ports        []uint32
	Annotations  map[string]string
	TlsEnabled   bool
	CertName     string
	UseClusterIp bool
}

func deepCopy(config *IngressServiceConfig) *IngressServiceConfig {
	return &IngressServiceConfig{
		Type:         config.Type,
		Ports:        slices.Clone(config.Ports),
		Annotations:  maps.Clone(config.Annotations),
		TlsEnabled:   config.TlsEnabled,
		CertName:     config.CertName,
		UseClusterIp: config.UseClusterIp,
	}
}

func CombineIngressService(ingresses []*api.IngressConfig, services []*api.ServiceConfig) []*IngressServiceConfig {
	result := []*IngressServiceConfig{}

	for _, ing := range ingresses {
		result = append(
			result,
			&IngressServiceConfig{
				Type:         Ingress,
				Ports:        slices.Clone(ing.Ports),
				Annotations:  maps.Clone(ing.Annotations),
				TlsEnabled:   ing.TlsEnabled,
				CertName:     ing.CertName,
				UseClusterIp: ing.UseClusterIP,
			},
		)
	}

	for _, svc := range services {
		svcType := NodePort
		useClusterIP := true
		if svc.Type == api.ServiceType_Headless {
			svcType = Headless
			useClusterIP = false
		}
		result = append(
			result,
			&IngressServiceConfig{
				Type:         svcType,
				Ports:        slices.Clone(svc.Ports),
				UseClusterIp: useClusterIP,
			},
		)
	}

	return result
}

func useClusterIP(configs []*IngressServiceConfig) bool {
	for _, config := range configs {
		if config.UseClusterIp {
			return true
		}
	}
	return false
}
