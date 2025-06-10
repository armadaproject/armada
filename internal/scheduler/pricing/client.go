package pricing

import (
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/pkg/bidstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func NewBidRetrieverServiceClient(config schedulerconfig.PricingApiConfig) (bidstore.BidRetrieverServiceClient, error) {
	creds := credentials.NewClientTLSFromCert(nil, "")
	if config.ForceNoTls {
		creds = insecure.NewCredentials()
	}
	client, err := grpc.NewClient(config.ServiceUrl, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, err
	}
	return bidstore.NewBidRetrieverServiceClient(client), nil
}
