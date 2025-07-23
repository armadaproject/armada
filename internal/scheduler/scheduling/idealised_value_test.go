package scheduling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/pricing"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/bidstore"
)

var (
	zeroResources = testfixtures.TestResourceListFactory.MakeAllZero()
	resourceUnit  = testfixtures.TestResourceListFactory.FromJobResourceListIgnoreUnknown(
		map[string]k8sResource.Quantity{
			"cpu": k8sResource.MustParse("1"),
		},
	)
)

func TestCalculateIdealisedValue(t *testing.T) {
	ctx := armadacontext.Background()

	tc := map[string]struct {
		queues             []string
		nodes              []*internaltypes.Node
		jobsByQueue        map[string][]*jobdb.Job
		runningJobsByQueue map[string][]*jobdb.Job
		expectedValues     map[string]float64
	}{
		"One Queue, all jobs scheduled": {
			queues: []string{"queueA"},
			nodes:  testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			jobsByQueue: map[string][]*jobdb.Job{
				"queueA": testfixtures.N16Cpu128GiJobs("queueA", testfixtures.PriorityClass0, 2),
			},
			expectedValues: map[string]float64{
				"queueA": 32, // two 16 core jobs scheduled @ 1
			},
		},
		"One Queue, some jobs scheduled": {
			queues: []string{"queueA"},
			nodes:  testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			jobsByQueue: map[string][]*jobdb.Job{
				"queueA": testfixtures.N16Cpu128GiJobs("queueA", testfixtures.PriorityClass0, 4),
			},
			expectedValues: map[string]float64{
				"queueA": 32, // two 16 core jobs scheduled @ 1
			},
		},
		"One Queue, job too large to fit on one node": {
			queues: []string{"queueA"},
			nodes:  testfixtures.N32CpuNodes(2, testfixtures.TestPriorities),
			jobsByQueue: map[string][]*jobdb.Job{
				"queueA": testfixtures.N64Cpu512GiJobs("queueA", testfixtures.PriorityClass0, 1),
			},
			expectedValues: map[string]float64{
				"queueA": 64, // one 64 core job scheduled @ 1
			},
		},
		"Two Queues, some jobs scheduled": {
			queues: []string{"queueA"},
			nodes:  testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			jobsByQueue: map[string][]*jobdb.Job{
				"queueA": testfixtures.N16Cpu128GiJobs("queueA", testfixtures.PriorityClass0, 1),
				"queueB": testfixtures.N16Cpu128GiJobs("queueB", testfixtures.PriorityClass0, 2),
			},
			expectedValues: map[string]float64{
				"queueA": 16, // one 16 core job scheduled @ 1
				"queueB": 16, // one 16 core job scheduled @ 1
			},
		},
		"One Queue, ignore gang constraints": {
			queues: []string{"queueA"},
			nodes:  testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			jobsByQueue: map[string][]*jobdb.Job{
				"queueA": testfixtures.WithNodeUniformityGangAnnotationsJobs(testfixtures.N16Cpu128GiJobs("queueA", testfixtures.PriorityClass0, 2), testfixtures.ClusterNameLabel),
			},
			expectedValues: map[string]float64{
				"queueA": 32, // one 32 core gang scheduled @ 1
			},
		},
		"One Queue, ignore node selectors": {
			queues: []string{"queueA"},
			nodes:  testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			jobsByQueue: map[string][]*jobdb.Job{
				"queueA": testfixtures.WithNodeSelectorJobs(
					map[string]string{
						"key": "value",
					},
					testfixtures.N16Cpu128GiJobs("queueA", testfixtures.PriorityClass0, 2),
				),
			},
			expectedValues: map[string]float64{
				"queueA": 32, // two 16 cores jobs scheduled @ 1
			},
		},
		"One Queue, running job": {
			queues: []string{"queueA"},
			nodes:  testfixtures.N32CpuNodes(1, testfixtures.TestPriorities),
			runningJobsByQueue: map[string][]*jobdb.Job{
				"queueA": testfixtures.N16Cpu128GiJobs("queueA", testfixtures.PriorityClass0, 2),
			},
			expectedValues: map[string]float64{
				"queueA": 64, // two 16 core jobs scheduled @ 2
			},
		},
	}

	for name, tc := range tc {
		t.Run(name, func(t *testing.T) {
			schedulingConfig := testfixtures.TestSchedulingConfig()
			schedulingConfig.Pools[0].ExperimentalMarketScheduling = &configuration.MarketSchedulingConfig{
				Enabled: true,
			}

			// Setup a scheduling context
			totalResources := zeroResources
			for _, node := range tc.nodes {
				totalResources = totalResources.Add(node.GetAllocatableResources())
			}

			fairnessCostProvider, err := fairness.NewDominantResourceFairness(
				totalResources,
				testfixtures.TestPool,
				testfixtures.TestSchedulingConfig(),
			)
			require.NoError(t, err)

			sctx := schedulercontext.NewSchedulingContext(
				testfixtures.TestPool,
				fairnessCostProvider,
				noOpRateLimiter,
				totalResources)

			for _, q := range tc.queues {
				err = sctx.AddQueueSchedulingContext(
					q,
					1.0,
					1.0,
					map[string]internaltypes.ResourceList{},
					zeroResources,
					zeroResources,
					zeroResources,
					noOpRateLimiter)
				require.NoError(t, err)
			}

			constraints := schedulerconstraints.NewSchedulingConstraints(
				testfixtures.TestPool,
				totalResources,
				schedulingConfig,
				armadaslices.Map(
					tc.queues,
					func(qn string) *api.Queue { return &api.Queue{Name: qn} },
				))

			allJobs := make([]*jobdb.Job, 0)
			for _, jobs := range tc.jobsByQueue {
				for _, job := range jobs {
					job = job.WithQueued(true).WithPriceBand(bidstore.PriceBand_PRICE_BAND_UNSPECIFIED).WithBidPrices(map[string]pricing.Bid{
						testfixtures.TestPool: {
							RunningBid: 2.0,
							QueuedBid:  1.0,
						},
					})
					allJobs = append(allJobs, job)
				}
			}

			for _, jobs := range tc.runningJobsByQueue {
				for _, job := range jobs {
					job = job.
						WithNewRun("executor-01", "some-node", "some-node", testfixtures.TestPool, job.PriorityClass().Priority).
						WithPriceBand(bidstore.PriceBand_PRICE_BAND_UNSPECIFIED).WithBidPrices(map[string]pricing.Bid{
						testfixtures.TestPool: {
							RunningBid: 2.0,
							QueuedBid:  1.0,
						},
					})
					allJobs = append(allJobs, job)
					tc.nodes[0].AllocatedByJobId[job.Id()] = job.KubernetesResourceRequirements()
				}
			}

			jobDb := testfixtures.NewJobDbWithJobs(allJobs)
			err = CalculateIdealisedValue(
				ctx,
				sctx,
				tc.nodes,
				jobDb.ReadTxn(),
				constraints,
				testfixtures.TestFloatingResources,
				schedulingConfig,
				testfixtures.TestResourceListFactory,
				resourceUnit,
			)
			require.NoError(t, err)

			for qName, qctx := range sctx.QueueSchedulingContexts {
				assert.Equal(t, tc.expectedValues[qName], qctx.IdealisedValue, qName)
			}
		})
	}
}
