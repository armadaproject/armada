package util

import (
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

type ServiceType int

const (
	Ingress ServiceType = iota
	NodePort
	Headless
)

func (st ServiceType) String() string {
	return []string{"Ingress", "NodePort", "Headless"}[st]
}

type ServiceConfig struct {
	Type        ServiceType
	Ports       []uint32
	Annotations map[string]string
	TlsEnabled  bool
	CertName    string
}

func CombineIngressService(ingresses []*api.IngressConfig, services []*api.ServiceConfig) []*ServiceConfig {
	result := []*ServiceConfig{}

	for _, ing := range ingresses {
		result = append(
			result,
			&ServiceConfig{
				Type:        Ingress,
				Ports:       util.DeepCopyListUint32(ing.Ports),
				Annotations: util.DeepCopy(ing.Annotations),
				TlsEnabled:  ing.TlsEnabled,
				CertName:    ing.CertName,
			},
		)
	}

	for _, svc := range services {
		svcType := NodePort
		if svc.Type == api.ServiceType_Headless {
			svcType = Headless
		}
		result = append(
			result,
			&ServiceConfig{
				Type:  svcType,
				Ports: util.DeepCopyListUint32(svc.Ports),
			},
		)
	}

	return result
}
