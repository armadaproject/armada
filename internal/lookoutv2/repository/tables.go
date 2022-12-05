package repository

import (
	"github.com/G-Research/armada/internal/lookoutv2/model"
	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/common/util"
)

const (
	jobTable                  = "job"
	jobRunTable               = "job_run"
	userAnnotationLookupTable = "user_annotation_lookup"

	jobTableAbbrev                  = "j"
	jobRunTableAbbrev               = "jr"
	userAnnotationLookupTableAbbrev = "ual"

	jobIdCol  = "job_id"
	queueCol  = "queue"
	jobSetCol = "jobset"
	stateCol  = "state"
	ownerCol  = "owner"

	annotationKeyCol   = "key"
	annotationValueCol = "value"
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
}

func NewTables() *LookoutTables {
	return &LookoutTables{
		fieldColumnMap: map[string]string{
			"jobId":  jobIdCol,
			"queue":  queueCol,
			"jobSet": jobSetCol,
			"owner":  ownerCol,
			"state":  stateCol,
		},
		columnsTableMap: map[string]map[string]bool{
			jobIdCol:  util.StringListToSet([]string{jobTable, jobRunTable, userAnnotationLookupTable}),
			queueCol:  util.StringListToSet([]string{jobTable, userAnnotationLookupTable}),
			jobSetCol: util.StringListToSet([]string{jobTable, userAnnotationLookupTable}),
			ownerCol:  util.StringListToSet([]string{jobTable}),
			stateCol:  util.StringListToSet([]string{jobTable}),
		},
		orderableColumns: util.StringListToSet([]string{
			jobIdCol,
		}),
		filterableColumns: map[string]map[string]bool{
			jobIdCol:  util.StringListToSet([]string{model.MatchExact}),
			queueCol:  util.StringListToSet([]string{model.MatchExact, model.MatchStartsWith}),
			jobSetCol: util.StringListToSet([]string{model.MatchExact, model.MatchStartsWith}),
			ownerCol:  util.StringListToSet([]string{model.MatchExact, model.MatchStartsWith}),
			stateCol:  util.StringListToSet([]string{model.MatchExact, model.MatchAnyOf}),
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
