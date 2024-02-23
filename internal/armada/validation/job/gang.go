package job

import (
	"fmt"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/pkg/errors"
)

type jobAdpter struct {
	*api.JobSubmitRequestItem
}

func (j jobAdpter) GetPriorityClassName() string {
	if j.PodSpec != nil {
		return j.PodSpec.PriorityClassName
	} else if len(j.PodSpecs) > 0 {
		return j.PodSpecs[0].PriorityClassName
	}
	return ""
}

type gangValidator struct{}

func (p gangValidator) Validate(request *api.JobSubmitRequest) error {
	gangDetailsByGangId := make(map[string]schedulercontext.GangInfo)
	for _, job := range request.JobRequestItems {
		actual, err := schedulercontext.GangInfoFromLegacySchedulerJob(jobAdpter{job})
		if err != nil {
			return fmt.Errorf("invalid gang annotations: %s", err.Error())
		}
		if actual.Id == "" {
			continue
		}
		if expected, ok := gangDetailsByGangId[actual.Id]; ok {
			if expected.Cardinality != actual.Cardinality {
				return errors.Errorf(
					"inconsistent gang cardinality in gang %s: expected %d but got %d",
					actual.Id, expected.Cardinality, actual.Cardinality,
				)
			}
			if expected.MinimumCardinality != actual.MinimumCardinality {
				return errors.Errorf(
					"inconsistent gang minimum cardinality in gang %s: expected %d but got %d",
					actual.Id, expected.MinimumCardinality, actual.MinimumCardinality,
				)
			}
			if expected.PriorityClassName != actual.PriorityClassName {
				return errors.Errorf(
					"inconsistent PriorityClassName in gang %s: expected %s but got %s",
					actual.Id, expected.PriorityClassName, actual.PriorityClassName,
				)
			}
			if actual.NodeUniformity != expected.NodeUniformity {
				return errors.Errorf(
					"inconsistent nodeUniformityLabel in gang %s: expected %s but got %s",
					actual.Id, expected.NodeUniformity, actual.NodeUniformity,
				)
			}
		} else {
			gangDetailsByGangId[actual.Id] = actual
		}
	}
	return nil
}
