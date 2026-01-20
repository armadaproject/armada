package jobspec

import (
	"fmt"

	"github.com/armadaproject/armada/internal/broadside/configuration"
)

const proportionScale = 1000

func positiveModulo(num int) int {
	result := num % proportionScale
	if result < 0 {
		result += proportionScale
	}
	return result
}

type proportionSelector struct {
	thresholds []int
}

func newProportionSelector(proportions []float64) *proportionSelector {
	thresholds := make([]int, len(proportions))
	cumulative := 0
	for i, p := range proportions {
		cumulative += int(p * float64(proportionScale))
		thresholds[i] = cumulative
	}
	return &proportionSelector{thresholds: thresholds}
}

func (s *proportionSelector) selectIndex(num int) int {
	position := positiveModulo(num)
	for i, threshold := range s.thresholds {
		if position < threshold {
			return i
		}
	}
	return len(s.thresholds) - 1
}

type QueueJobSetSelector struct {
	queueSelector   *proportionSelector
	jobSetSelectors []*proportionSelector
}

func NewQueueJobSetSelector(queueConfigs []configuration.QueueConfig) (*QueueJobSetSelector, error) {
	if len(queueConfigs) == 0 {
		return nil, fmt.Errorf("queueConfigs must not be empty")
	}

	queueProportions := make([]float64, len(queueConfigs))
	for i, q := range queueConfigs {
		queueProportions[i] = q.Proportion
	}

	jobSetSelectors := make([]*proportionSelector, len(queueConfigs))
	for i, q := range queueConfigs {
		if len(q.JobSetConfig) == 0 {
			return nil, fmt.Errorf("queue %s must have at least one job set", q.Name)
		}
		jsProportions := make([]float64, len(q.JobSetConfig))
		for j, js := range q.JobSetConfig {
			jsProportions[j] = js.Proportion
		}
		jobSetSelectors[i] = newProportionSelector(jsProportions)
	}

	return &QueueJobSetSelector{
		queueSelector:   newProportionSelector(queueProportions),
		jobSetSelectors: jobSetSelectors,
	}, nil
}

func (s *QueueJobSetSelector) SelectQueueAndJobSet(jobNumber int) (queueIdx, jobSetIdx int) {
	queueIdx = s.queueSelector.selectIndex(jobNumber)
	jobSetIdx = s.jobSetSelectors[queueIdx].selectIndex(jobNumber)
	return queueIdx, jobSetIdx
}

func ShouldSucceed(jobNumber int, cfg configuration.JobStateTransitionConfig) bool {
	threshold := int(cfg.ProportionSucceed * float64(proportionScale))
	return positiveModulo(jobNumber) < threshold
}

func DetermineHistoricalState(jobNum int, cfg configuration.HistoricalJobsConfig) JobState {
	succeededThreshold := int(cfg.ProportionSucceeded * float64(proportionScale))
	erroredThreshold := succeededThreshold + int(cfg.ProportionErrored*float64(proportionScale))
	cancelledThreshold := erroredThreshold + int(cfg.ProportionCancelled*float64(proportionScale))

	position := positiveModulo(jobNum)
	switch {
	case position < succeededThreshold:
		return StateSucceeded
	case position < erroredThreshold:
		return StateErrored
	case position < cancelledThreshold:
		return StateCancelled
	default:
		return StatePreempted
	}
}
