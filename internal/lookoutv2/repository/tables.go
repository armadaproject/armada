package repository

import (
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
)

const (
	stateField              = "state"
	submittedField          = "submitted"
	lastTransitionTimeField = "lastTransitionTime"

	jobTable    = "job"
	jobRunTable = "job_run"

	jobTableAbbrev    = "j"
	jobRunTableAbbrev = "jr"

	jobIdCol              = "job_id"
	queueCol              = "queue"
	namespaceCol          = "namespace"
	jobSetCol             = "jobset"
	stateCol              = "state"
	ownerCol              = "owner"
	cpuCol                = "cpu"
	memoryCol             = "memory"
	ephemeralStorageCol   = "ephemeral_storage"
	gpuCol                = "gpu"
	priorityCol           = "priority"
	submittedCol          = "submitted"
	lastTransitionTimeCol = "last_transition_time_seconds"
	priorityClassCol      = "priority_class"
)

type AggregateType int

const (
	Unknown     AggregateType = -1
	Max                       = 0
	Average                   = 1
	StateCounts               = 2
	Min                       = 3
)

type LookoutTables struct {
	// field name -> column name
	fieldColumnMap map[string]string
	// set of column names that can be ordered
	orderableColumns map[string]bool
	// column name -> set of supported matches for column
	filterableColumns map[string]map[string]bool
	// table name -> abbreviated table name
	tableAbbrevs map[string]string
	// columns that can be grouped by
	groupableColumns map[string]bool
	// map from column to aggregate that can be performed on it
	groupAggregates map[string]AggregateType
}

func NewTables() *LookoutTables {
	return &LookoutTables{
		fieldColumnMap: map[string]string{
			"jobId":              jobIdCol,
			"queue":              queueCol,
			"jobSet":             jobSetCol,
			"owner":              ownerCol,
			"namespace":          namespaceCol,
			"state":              stateCol,
			"cpu":                cpuCol,
			"memory":             memoryCol,
			"ephemeralStorage":   ephemeralStorageCol,
			"gpu":                gpuCol,
			"priority":           priorityCol,
			"submitted":          submittedCol,
			"lastTransitionTime": lastTransitionTimeCol,
			"priorityClass":      priorityClassCol,
		},
		orderableColumns: util.StringListToSet([]string{
			jobIdCol,
			jobSetCol,
			submittedCol,
			lastTransitionTimeCol,
		}),
		filterableColumns: map[string]map[string]bool{
			jobIdCol:            util.StringListToSet([]string{model.MatchExact}),
			queueCol:            util.StringListToSet([]string{model.MatchExact, model.MatchStartsWith, model.MatchContains}),
			jobSetCol:           util.StringListToSet([]string{model.MatchExact, model.MatchStartsWith, model.MatchContains}),
			ownerCol:            util.StringListToSet([]string{model.MatchExact, model.MatchStartsWith, model.MatchContains}),
			namespaceCol:        util.StringListToSet([]string{model.MatchExact, model.MatchStartsWith, model.MatchContains}),
			stateCol:            util.StringListToSet([]string{model.MatchExact, model.MatchAnyOf}),
			cpuCol:              util.StringListToSet([]string{model.MatchExact, model.MatchGreaterThan, model.MatchLessThan, model.MatchGreaterThanOrEqualTo, model.MatchLessThanOrEqualTo}),
			memoryCol:           util.StringListToSet([]string{model.MatchExact, model.MatchGreaterThan, model.MatchLessThan, model.MatchGreaterThanOrEqualTo, model.MatchLessThanOrEqualTo}),
			ephemeralStorageCol: util.StringListToSet([]string{model.MatchExact, model.MatchGreaterThan, model.MatchLessThan, model.MatchGreaterThanOrEqualTo, model.MatchLessThanOrEqualTo}),
			gpuCol:              util.StringListToSet([]string{model.MatchExact, model.MatchGreaterThan, model.MatchLessThan, model.MatchGreaterThanOrEqualTo, model.MatchLessThanOrEqualTo}),
			priorityCol:         util.StringListToSet([]string{model.MatchExact, model.MatchGreaterThan, model.MatchLessThan, model.MatchGreaterThanOrEqualTo, model.MatchLessThanOrEqualTo}),
			priorityClassCol:    util.StringListToSet([]string{model.MatchExact, model.MatchStartsWith, model.MatchContains}),
		},
		tableAbbrevs: map[string]string{
			jobTable:    jobTableAbbrev,
			jobRunTable: jobRunTableAbbrev,
		},
		groupableColumns: util.StringListToSet([]string{
			queueCol,
			namespaceCol,
			jobSetCol,
			stateCol,
		}),
		groupAggregates: map[string]AggregateType{
			submittedCol:          Min,
			lastTransitionTimeCol: Average,
			stateCol:              StateCounts,
		},
	}
}

func (c *LookoutTables) ColumnFromField(field string) (string, error) {
	col, ok := c.fieldColumnMap[field]
	if !ok {
		return "", errors.Errorf("column for field %s not found", field)
	}
	return col, nil
}

func (c *LookoutTables) IsOrderable(col string) bool {
	_, ok := c.orderableColumns[col]
	return ok
}

func (c *LookoutTables) IsFilterable(col string) bool {
	_, ok := c.filterableColumns[col]
	return ok
}

func (c *LookoutTables) SupportsMatch(col, match string) bool {
	supportedMatches, ok := c.filterableColumns[col]
	if !ok {
		return false
	}
	_, isSupported := supportedMatches[match]
	return isSupported
}

func (c *LookoutTables) TableAbbrev(table string) (string, error) {
	abbrev, ok := c.tableAbbrevs[table]
	if !ok {
		return "", errors.Errorf("abbreviation for table %s not found", table)
	}
	return abbrev, nil
}

func (c *LookoutTables) IsGroupable(col string) bool {
	_, ok := c.groupableColumns[col]
	return ok
}

func (c *LookoutTables) GroupAggregateForCol(col string) (AggregateType, error) {
	aggregate, ok := c.groupAggregates[col]
	if !ok {
		return Unknown, errors.Errorf("no aggregate found for column %s", col)
	}
	return aggregate, nil
}
