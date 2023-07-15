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

	jobTable                  = "job"
	jobRunTable               = "job_run"
	userAnnotationLookupTable = "user_annotation_lookup"

	jobTableAbbrev                  = "j"
	jobRunTableAbbrev               = "jr"
	userAnnotationLookupTableAbbrev = "ual"

	jobIdCol              = "job_id"
	queueCol              = "queue"
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

	annotationKeyCol   = "key"
	annotationValueCol = "value"
)

type AggregateType int

const (
	Unknown     AggregateType = -1
	Max                       = 0
	Average                   = 1
	StateCounts               = 2
)

type LookoutTables struct {
	// field name -> column name
	fieldColumnMap map[string]string
	// column name -> set of tables with that column
	// (the same column could be in multiple tables, either as a foreign key or for denormalization)
	columnsTableMap map[string]map[string]bool
	// set of column names that can be ordered
	orderableColumns map[string]bool
	// column name -> set of supported matches for column
	filterableColumns map[string]map[string]bool
	// table name -> abbreviated table name
	tableAbbrevs map[string]string
	// order of precedence for tables - which tables to prioritize querying from
	tablePrecedence []string
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
		columnsTableMap: map[string]map[string]bool{
			jobIdCol:              util.StringListToSet([]string{jobTable, jobRunTable, userAnnotationLookupTable}),
			queueCol:              util.StringListToSet([]string{jobTable, userAnnotationLookupTable}),
			jobSetCol:             util.StringListToSet([]string{jobTable, userAnnotationLookupTable}),
			ownerCol:              util.StringListToSet([]string{jobTable}),
			stateCol:              util.StringListToSet([]string{jobTable}),
			cpuCol:                util.StringListToSet([]string{jobTable}),
			memoryCol:             util.StringListToSet([]string{jobTable}),
			ephemeralStorageCol:   util.StringListToSet([]string{jobTable}),
			gpuCol:                util.StringListToSet([]string{jobTable}),
			priorityCol:           util.StringListToSet([]string{jobTable}),
			submittedCol:          util.StringListToSet([]string{jobTable}),
			lastTransitionTimeCol: util.StringListToSet([]string{jobTable}),
			priorityClassCol:      util.StringListToSet([]string{jobTable}),
		},
		orderableColumns: util.StringListToSet([]string{
			jobIdCol,
			submittedCol,
			lastTransitionTimeCol,
		}),
		filterableColumns: map[string]map[string]bool{
			jobIdCol:            util.StringListToSet([]string{model.MatchExact}),
			queueCol:            util.StringListToSet([]string{model.MatchExact, model.MatchStartsWith, model.MatchContains}),
			jobSetCol:           util.StringListToSet([]string{model.MatchExact, model.MatchStartsWith, model.MatchContains}),
			ownerCol:            util.StringListToSet([]string{model.MatchExact, model.MatchStartsWith, model.MatchContains}),
			stateCol:            util.StringListToSet([]string{model.MatchExact, model.MatchAnyOf}),
			cpuCol:              util.StringListToSet([]string{model.MatchExact, model.MatchGreaterThan, model.MatchLessThan, model.MatchGreaterThanOrEqualTo, model.MatchLessThanOrEqualTo}),
			memoryCol:           util.StringListToSet([]string{model.MatchExact, model.MatchGreaterThan, model.MatchLessThan, model.MatchGreaterThanOrEqualTo, model.MatchLessThanOrEqualTo}),
			ephemeralStorageCol: util.StringListToSet([]string{model.MatchExact, model.MatchGreaterThan, model.MatchLessThan, model.MatchGreaterThanOrEqualTo, model.MatchLessThanOrEqualTo}),
			gpuCol:              util.StringListToSet([]string{model.MatchExact, model.MatchGreaterThan, model.MatchLessThan, model.MatchGreaterThanOrEqualTo, model.MatchLessThanOrEqualTo}),
			priorityCol:         util.StringListToSet([]string{model.MatchExact, model.MatchGreaterThan, model.MatchLessThan, model.MatchGreaterThanOrEqualTo, model.MatchLessThanOrEqualTo}),
			priorityClassCol:    util.StringListToSet([]string{model.MatchExact, model.MatchStartsWith, model.MatchContains}),
		},
		tableAbbrevs: map[string]string{
			jobTable:                  jobTableAbbrev,
			jobRunTable:               jobRunTableAbbrev,
			userAnnotationLookupTable: userAnnotationLookupTableAbbrev,
		},
		tablePrecedence: []string{
			jobTable,
			jobRunTable,
			userAnnotationLookupTable,
		},
		groupableColumns: util.StringListToSet([]string{
			queueCol,
			jobSetCol,
			stateCol,
		}),
		groupAggregates: map[string]AggregateType{
			submittedCol:          Max,
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

func (c *LookoutTables) TablesForColumn(col string) (map[string]bool, error) {
	tables, ok := c.columnsTableMap[col]
	if !ok {
		return nil, errors.Errorf("cannot find table for column %s", col)
	}
	return tables, nil
}

func (c *LookoutTables) TableAbbrev(table string) (string, error) {
	abbrev, ok := c.tableAbbrevs[table]
	if !ok {
		return "", errors.Errorf("abbreviation for table %s not found", table)
	}
	return abbrev, nil
}

func (c *LookoutTables) TablePrecedence() []string {
	return c.tablePrecedence
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
