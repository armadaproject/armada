package prioritymultiplier

import (
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"sync/atomic"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/pkg/priorityoverride"
)

func NewServiceClient(config schedulerconfig.PriorityMultiplierConfig) (priorityoverride.PriorityMultiplierServiceClient, error) {
	creds := credentials.NewClientTLSFromCert(nil, "")
	if config.ForceNoTls {
		creds = insecure.NewCredentials()
	}
	client, err := grpc.NewClient(config.ServiceUrl, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}
	return priorityoverride.NewPriorityMultiplierServiceClient(client), nil
}

// ServiceProvider is an implementation of Provider that fetches priority multipliers from the Priority Multiplier Service.
// We cache the multipliers in memory so that we can continue scheduling even if the API is unavailable
type ServiceProvider struct {
	updateFrequency time.Duration
	apiClient       priorityoverride.PriorityMultiplierServiceClient
	multipliers     atomic.Pointer[map[multiplierKey]float64]
}

func NewServiceProvider(apiClient priorityoverride.PriorityMultiplierServiceClient, updateFrequency time.Duration) *ServiceProvider {
	return &ServiceProvider{
		updateFrequency: updateFrequency,
		apiClient:       apiClient,
		multipliers:     atomic.Pointer[map[multiplierKey]float64]{},
	}
}

func (p *ServiceProvider) Run(ctx *armadacontext.Context) error {
	if err := p.fetchMultipliers(ctx); err != nil {
		ctx.Warnf("Error fetching multipliers: %v", err)
	}
	ticker := time.NewTicker(p.updateFrequency)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.fetchMultipliers(ctx); err != nil {
				ctx.Warnf("Error fetching multipliers: %v", err)
			}
		}
	}
}

func (p *ServiceProvider) Ready() bool {
	return p.multipliers.Load() != nil
}

func (p *ServiceProvider) Multiplier(pool, queue string) (float64, error) {
	multipliers := p.multipliers.Load()
	if multipliers == nil {
		return 0, fmt.Errorf("no multipliers available")
	}
	multiplier, ok := (*multipliers)[multiplierKey{pool: pool, queue: queue}]
	if !ok {
		return 1.0, nil
	}
	return multiplier, nil
}

func (p *ServiceProvider) fetchMultipliers(ctx *armadacontext.Context) error {
	resp, err := p.apiClient.GetPriorityMultipliers(ctx, &priorityoverride.PriorityMultiplierRequest{})
	if err != nil {
		return err
	}
	multipliers := make(map[multiplierKey]float64)
	for _, poolMultipliers := range resp.PoolPriorityMultipliers {
		for queue, multiplier := range poolMultipliers.Multipliers {
			key := multiplierKey{pool: poolMultipliers.Pool, queue: queue}
			multipliers[key] = multiplier
		}
	}
	return nil
}
