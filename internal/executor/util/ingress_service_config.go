package util

import (
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
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
	Type        IngressServiceType
	Ports       []uint32
	Annotations map[string]string
	TlsEnabled  bool
	CertName    string
}

func CombineIngressService(ingresses []*api.IngressConfig, services []*api.ServiceConfig) []*IngressServiceConfig {
	result := []*IngressServiceConfig{}

	for _, ing := range ingresses {
		result = append(
			result,
			&IngressServiceConfig{
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
			&IngressServiceConfig{
				Type:  svcType,
				Ports: util.DeepCopyListUint32(svc.Ports),
			},
		)
	}

	return result
}
