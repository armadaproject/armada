package repository

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
	"github.com/pkg/errors"
)

type QueryAggregator interface {
	AggregateSql() (string, error)
}

type SqlFunctionAggregator struct {
	queryCol    *queryColumn
	sqlFunction string
}

func NewSqlFunctionAggregator(queryCol *queryColumn, fn string) *SqlFunctionAggregator {
	return &SqlFunctionAggregator{
		queryCol:    queryCol,
		sqlFunction: fn,
	}
}

func (qa *SqlFunctionAggregator) aggregateColName() string {
	return qa.queryCol.name
}

func (qa *SqlFunctionAggregator) AggregateSql() (string, error) {
	return fmt.Sprintf("%s(%s.%s) AS %s", qa.sqlFunction, qa.queryCol.abbrev, qa.queryCol.name, qa.aggregateColName()), nil
}

type StateCountAggregator struct {
	queryCol    *queryColumn
	stateString string
}

func NewStateCountAggregator(queryCol *queryColumn, stateString string) *StateCountAggregator {
	return &StateCountAggregator{
		queryCol:    queryCol,
		stateString: stateString,
	}
}

func (qa *StateCountAggregator) aggregateColName() string {
	return fmt.Sprintf("%s_%s", qa.queryCol.name, qa.stateString)
}

func (qa *StateCountAggregator) AggregateSql() (string, error) {
	stateInt, ok := lookout.JobStateOrdinalMap[lookout.JobState(qa.stateString)]
	if !ok {
		return "", errors.Errorf("state %s does not exist", qa.stateString)
	}
	return fmt.Sprintf(
		"SUM(CASE WHEN %s.%s = %d THEN 1 ELSE 0 END) AS %s",
		qa.queryCol.abbrev, qa.queryCol.name, stateInt, qa.aggregateColName(),
	), nil
}

func GetAggregatorsForColumn(queryCol *queryColumn, aggregateType AggregateType, filters []*model.Filter) ([]QueryAggregator, error) {
	switch aggregateType {
	case Max:
		return []QueryAggregator{NewSqlFunctionAggregator(queryCol, "MAX")}, nil
	case Average:
		return []QueryAggregator{NewSqlFunctionAggregator(queryCol, "AVG")}, nil
	case StateCounts:
		states := GetStatesForFilter(filters)
		aggregators := make([]QueryAggregator, len(states))
		for i, state := range states {
			aggregators[i] = NewStateCountAggregator(queryCol, state)
		}
		return aggregators, nil
	default:
		return nil, errors.Errorf("cannot determine aggregate type: %v", aggregateType)
	}
}

// GetStatesForFilter returns a list of states as string if filter for state exists
// Will always return the states in the same order, irrespective of the ordering of the states in the filter
func GetStatesForFilter(filters []*model.Filter) []string {
	var stateFilter *model.Filter
	for _, f := range filters {
		if f.Field == stateField {
			stateFilter = f
		}
	}
	allStates := util.Map(lookout.JobStates, func(jobState lookout.JobState) string { return string(jobState) })
	if stateFilter == nil {
		// If no state filter is specified, use all states
		return allStates
	}

	switch stateFilter.Match {
	case model.MatchExact:
		return []string{fmt.Sprintf("%s", stateFilter.Value)}
	case model.MatchAnyOf:
		strSlice, err := toStringSlice(stateFilter.Value)
		if err != nil {
			return allStates
		}
		stateStringSet := util.StringListToSet(strSlice)
		// Ensuring they are in the same order
		var finalStates []string
		for _, state := range allStates {
			if _, ok := stateStringSet[state]; ok {
				finalStates = append(finalStates, state)
			}
		}
		return finalStates
	default:
		return allStates
	}
}

func toStringSlice(val interface{}) ([]string, error) {
	switch v := val.(type) {
	case []string:
		return v, nil
	case []interface{}:
		result := make([]string, len(v))
		for i := 0; i < len(v); i++ {
			str := fmt.Sprintf("%v", v[i])
			result[i] = str
		}
		return result, nil
	default:
		return nil, errors.Errorf("failed to convert interface to string slice: %v of type %T", val, val)
	}
}
