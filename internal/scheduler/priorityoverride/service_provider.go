package priorityoverride

import (
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/pkg/priorityoverride"
)

func NewServiceClient(config schedulerconfig.PriorityOverrideConfig) (priorityoverride.PriorityOverrideServiceClient, error) {
	creds := credentials.NewClientTLSFromCert(nil, "")
	if config.ForceNoTls {
		creds = insecure.NewCredentials()
	}
	client, err := grpc.NewClient(config.ServiceUrl, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}
	return priorityoverride.NewPriorityOverrideServiceClient(client), nil
}

// ServiceProvider is an implementation of Provider that fetches priority overrides from the Priority Overrides Service.
// We cache the overrides in memory so that we can continue scheduling even if the API is unavailable
type ServiceProvider struct {
	updateFrequency time.Duration
	apiClient       priorityoverride.PriorityOverrideServiceClient
	overrides       atomic.Pointer[map[overrideKey]float64]
}

func NewServiceProvider(apiClient priorityoverride.PriorityOverrideServiceClient, updateFrequency time.Duration) *ServiceProvider {
	return &ServiceProvider{
		updateFrequency: updateFrequency,
		apiClient:       apiClient,
		overrides:       atomic.Pointer[map[overrideKey]float64]{},
	}
}

func (p *ServiceProvider) Run(ctx *armadacontext.Context) error {
	if err := p.fetchOverrides(ctx); err != nil {
		ctx.Warnf("Error fetching overrides: %v", err)
	}
	ticker := time.NewTicker(p.updateFrequency)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.fetchOverrides(ctx); err != nil {
				ctx.Warnf("Error fetching overrides: %v", err)
			}
		}
	}
}

func (p *ServiceProvider) Ready() bool {
	return p.overrides.Load() != nil
}

func (p *ServiceProvider) Override(pool, queue string) (float64, bool, error) {
	multipliers := p.overrides.Load()
	if multipliers == nil {
		return 0, false, fmt.Errorf("no overrides available")
	}
	multiplier, ok := (*multipliers)[overrideKey{pool: pool, queue: queue}]
	return multiplier, ok, nil
}

func (p *ServiceProvider) fetchOverrides(ctx *armadacontext.Context) error {
	resp, err := p.apiClient.GetPriorityOverrides(ctx, &priorityoverride.PriorityOverrideRequest{})
	if err != nil {
		return err
	}
	overrides := make(map[overrideKey]float64)
	for _, poolOverrides := range resp.PoolPriorityOverrides {
		for queue, override := range poolOverrides.Overrides {
			key := overrideKey{pool: poolOverrides.Pool, queue: queue}
			overrides[key] = override
		}
	}
	p.overrides.Store(&overrides)
	return nil
}
