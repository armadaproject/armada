package submit

import (
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/server"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/scheduler"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/pkg/api"
)

func TestSubmit(t *testing.T) {
	defaultSchedulingConfig := configuration.SubmissionConfig{
		AllowedPriorityClassNames: map[string]bool{"pc1": true, "pc2": true},
		DefaultPriorityClassName:  "pc1",
		DefaultJobLimits:          armadaresource.ComputeResources{"cpu": resource.MustParse("1")},
		DefaultJobTolerations: []v1.Toleration{
			{
				Key:      "armadaproject.io/foo",
				Operator: "Exists",
			},
		},
		DefaultJobTolerationsByResourceRequest: map[string][]v1.Toleration{
			"nvidia.com/gpu": {
				{
					Key:      "armadaproject.io/gpuNode",
					Operator: "Exists",
				},
			},
		},
		MaxPodSpecSizeBytes:            1000,
		MinJobResources:                map[v1.ResourceName]resource.Quantity{},
		DefaultGangNodeUniformityLabel: "",
		MinTerminationGracePeriod:      30 * time.Second,
		MaxTerminationGracePeriod:      300 * time.Second,
		DefaultActiveDeadline:          1 * time.Hour,
		DefaultActiveDeadlineByResourceRequest: map[string]time.Duration{
			"nvidia.com/gpu": 24 * time.Hour,
		},
	}

	tests := map[string]struct {
		req              *api.JobSubmitRequest
		schedulingConfig configuration.SubmissionConfig
		expectSuccess    bool
	}{
		"valid request": {
			schedulingConfig: defaultSchedulingConfig,
			req: &api.JobSubmitRequest{
				Queue:    "testQueue",
				JobSetId: "testJobset",
				JobRequestItems: []*api.JobSubmitRequestItem{
					{
						Priority:    1000,
						Namespace:   "testNamespace",
						PodSpec: &v1.PodSpec{
							Containers: []v1.Container{
								{
									Resources: v1.ResourceRequirements{
										Requests: v1.ResourceList{
											"cpu":    resource.MustParse("1"),
											"memory": resource.MustParse("1Gi"),
										},
										Limits: v1.ResourceList{
											"cpu":    resource.MustParse("1"),
											"memory": resource.MustParse("1Gi"),
										},
									},
								},
							},
						},
					},
				},
			},
			expectSuccess: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			server := NewServer(publisher,
				queueRepository repository.QueueRepository,
				jobRepository repository.JobRepository,
				submissionConfig configuration.SubmissionConfig,
				deduplicator Deduplicator,
				submitChecker *scheduler.SubmitChecker,
				authorizer server.ActionAuthorizer)

			server.SubmitJobs()
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
