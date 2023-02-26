package service

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/mocks"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

func TestLeaseJobRuns(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockExecutorApiClient := mocks.NewMockExecutorApiClient(ctrl)
	mockStream := mocks.NewMockExecutorApi_LeaseJobRunsClient(ctrl)
	clusterIdentity := fake.NewFakeClusterIdentity("cluster-id", "cluster-pool")
	minimumJobSize := armadaresource.ComputeResources{
		"cpu":    resource.MustParse("1"),
		"memory": resource.MustParse("640Ki"),
	}

	message := &executorapi.LeaseStreamMessage{
		Event: &executorapi.LeaseStreamMessage_End{
			End: &executorapi.EndMarker{},
		},
	}

	cancelIds := &executorapi.LeaseStreamMessage{
		Event: &executorapi.LeaseStreamMessage_CancelRuns{
			CancelRuns: &executorapi.CancelRuns{
				JobRunIdsToCancel: []*armadaevents.Uuid{
					armadaevents.ProtoUuidFromUuid(uuid.New()),
				},
			},
		},
	}

	mockExecutorApiClient.EXPECT().LeaseJobRuns(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockStream, nil)
	mockStream.EXPECT().Send(gomock.Any()).Return(nil)
	mockStream.EXPECT().Recv().Return(cancelIds, nil)
	mockStream.EXPECT().Recv().Return(message, nil)
	jobLeaseRequester := NewJobLeaseRequester(mockExecutorApiClient, clusterIdentity, minimumJobSize)

	leases, idsToCancel, err := jobLeaseRequester.LeaseJobRuns(&LeaseRequest{})
	assert.NoError(t, err)
	assert.Len(t, leases, 0)
	assert.Len(t, idsToCancel, 1)
}
