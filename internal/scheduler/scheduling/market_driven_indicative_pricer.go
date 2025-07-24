package scheduling

import (
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/pricer"
	armadaconfiguration "github.com/armadaproject/armada/internal/server/configuration"
)

type IndicativeGangPricesByJobShape map[string]pricer.GangPricingResult

const (
	GangExceedsAllocatableUnschedulableReason = "The requested gang resources exceed the available capacity for scheduling"
	GangAllJobsEvictedUnschedulableReason     = "All jobs in the gang are already evicted"
	GangCardinalityZeroUnschedulableReason    = "The gang has cardinality zero"
)

type MarketDrivenIndicativePricer struct {
	gangPricer            *pricer.GangPricer
	jobDb                 jobdb.JobRepository
	constraints           schedulerconstraints.SchedulingConstraints
	floatingResourceTypes *floatingresources.FloatingResourceTypes
}

func NewMarketDrivenIndicativePricer(
	jobDb jobdb.JobRepository,
	gangPricer *pricer.GangPricer,
	constraints schedulerconstraints.SchedulingConstraints,
	floatingResourceTypes *floatingresources.FloatingResourceTypes,
) *MarketDrivenIndicativePricer {
	return &MarketDrivenIndicativePricer{
		gangPricer:            gangPricer,
		jobDb:                 jobDb,
		constraints:           constraints,
		floatingResourceTypes: floatingResourceTypes,
	}
}

// Price
// This method takes in a set of job shapes and returns a job scheduling summary GangPricingResult, which describes whether
// the job can be scheduled and if so, the minimum price at which that would be possible given the current market driven
// compute allocation. Invoking Price has no side-effects.
func (ip *MarketDrivenIndicativePricer) Price(ctx *armadacontext.Context, sctx *schedulercontext.SchedulingContext, jobRepo jobdb.JobRepository, gangsToPrice map[string]configuration.GangDefinition) (IndicativeGangPricesByJobShape, error) {
	factory := sctx.TotalResources.Factory()
	indicativePricesByJobShape := IndicativeGangPricesByJobShape{}
	maximumResourceToSchedule := sctx.TotalResources
	pricerIsTerminating := false

	gctxs, err := ip.gangContextsFromGangDefinitions(sctx.Pool, gangsToPrice, jobRepo, factory)
	if err != nil {
		return nil, err
	}

	for gangName, gctx := range gctxs {
		if pricerIsTerminating {
			indicativePricesByJobShape[gangName] = pricer.GangPricingResult{
				Evaluated:           false,
				Schedulable:         false,
				Price:               0.0,
				UnschedulableReason: "",
			}
			continue
		}
		if gctx.Cardinality() == 0 {
			indicativePricesByJobShape[gangName] = pricer.GangPricingResult{
				Evaluated:           true,
				Schedulable:         false,
				Price:               0.0,
				UnschedulableReason: GangCardinalityZeroUnschedulableReason,
			}
			continue
		}

		if gctx.TotalResourceRequests.Exceeds(maximumResourceToSchedule) {
			indicativePricesByJobShape[gangName] = pricer.GangPricingResult{
				Evaluated:           true,
				Schedulable:         false,
				Price:               0.0,
				UnschedulableReason: GangExceedsAllocatableUnschedulableReason,
			}
			continue
		}

		ok, reason, err := ip.checkIfWillBreachSchedulingLimits(gctx, sctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			if schedulerconstraints.IsTerminalUnschedulableReason(reason) {
				// Stop iterating if global limit hit
				break
			}
			indicativePricesByJobShape[gangName] = pricer.GangPricingResult{
				Evaluated:           true,
				Schedulable:         false,
				Price:               0.0,
				UnschedulableReason: reason,
			}
			continue
		}
		if hasContextExpired(ctx) || isNearDeadline(ctx) {
			pricerIsTerminating = true
			indicativePricesByJobShape[gangName] = pricer.GangPricingResult{
				Evaluated:           false,
				Schedulable:         false,
				Price:               0.0,
				UnschedulableReason: "",
			}
			continue
		}
		gangPricingResult, err := ip.gangPricer.Price(gctx)
		if err != nil {
			return nil, err
		}

		indicativePricesByJobShape[gangName] = gangPricingResult
	}
	return indicativePricesByJobShape, nil
}

func (ip *MarketDrivenIndicativePricer) gangContextsFromGangDefinitions(
	pool string,
	gangsToPrice map[string]configuration.GangDefinition,
	jobRepo jobdb.JobRepository,
	factory *internaltypes.ResourceListFactory,
) (map[string]*schedulercontext.GangSchedulingContext, error) {
	contexts := map[string]*schedulercontext.GangSchedulingContext{}

	for shapeName, definition := range gangsToPrice {
		// Under a market driven scheduler the queue and jobset have no bearing on resulting price.
		queue := "armada-market-driven-indicative-pricer"
		jobSet := "job-set-a"
		jctxs := make([]*schedulercontext.JobSchedulingContext, definition.Size)
		gangAnnotations := map[string]string{}
		gangId := util.NewULID()
		if definition.Size > 1 {
			gangAnnotations[armadaconfiguration.GangIdAnnotation] = gangId
			gangAnnotations[armadaconfiguration.GangCardinalityAnnotation] = fmt.Sprintf("%d", definition.Size)
			gangAnnotations[armadaconfiguration.GangNodeUniformityLabelAnnotation] = definition.NodeUniformity
		}

		for i := int32(0); i < definition.Size; i++ {
			jobID := util.NewULID()
			jobResources := definition.Resources.DeepCopy()
			podRequirements := &internaltypes.PodRequirements{
				Annotations:  gangAnnotations,
				NodeSelector: definition.NodeSelector,
				Tolerations:  definition.Tolerations,
				ResourceRequirements: v1.ResourceRequirements{
					Limits:   jobResources.AsKubernetesResourceList(),
					Requests: jobResources.AsKubernetesResourceList(),
				},
			}
			kubernetesResourceRequirements, err := factory.FromJobResourceListFailOnUnknown(*definition.Resources)
			if err != nil {
				return nil, err
			}
			job, err := jobRepo.NewJob(
				jobID,
				jobSet,
				queue,
				0,
				&internaltypes.JobSchedulingInfo{
					Lifetime:        1,
					PriorityClass:   definition.PriorityClassName,
					SubmitTime:      time.Now(),
					Priority:        0,
					PodRequirements: podRequirements,
					Version:         0,
				},
				true,
				0,
				false,
				false,
				false,
				0,
				false,
				[]string{pool},
				0,
			)
			if err != nil {
				return nil, err
			}
			jctxs[i] = &schedulercontext.JobSchedulingContext{
				Created:                        time.Now(),
				JobId:                          jobID,
				IsEvicted:                      false,
				Job:                            job,
				PodRequirements:                podRequirements,
				KubernetesResourceRequirements: kubernetesResourceRequirements,
				AdditionalNodeSelectors:        nil,
				AdditionalTolerations:          nil,
				UnschedulableReason:            "",
				PodSchedulingContext:           nil,
				AssignedNode:                   nil,
				PreemptingJob:                  nil,
				PreemptionType:                 "",
				PreemptionDescription:          "",
				Billable:                       false,
			}
		}

		contexts[shapeName] = schedulercontext.NewGangSchedulingContext(jctxs)
	}

	return contexts, nil
}

func (ip *MarketDrivenIndicativePricer) checkIfWillBreachSchedulingLimits(
	gctx *schedulercontext.GangSchedulingContext,
	sctx *schedulercontext.SchedulingContext,
) (bool, string, error) {
	ok, unschedulableReason, err := ip.constraints.CheckRoundConstraints(sctx)
	if err != nil || !ok {
		return false, unschedulableReason, err
	}

	if gctx.RequestsFloatingResources {
		ok, unschedulableReason = ip.floatingResourceTypes.WithinLimits(sctx.Pool, sctx.Allocated)
		if !ok {
			return ok, unschedulableReason, nil
		}
	}

	return true, "", nil
}
