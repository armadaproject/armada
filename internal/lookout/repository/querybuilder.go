package repository

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/database/lookout"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/lookout/model"
)

const (
	countCol                 = "count"
	activeJobSetsTableAbbrev = "active_job_sets"
)

var (
	activeJobSetsTable = fmt.Sprintf(
		`(
	SELECT DISTINCT %s, %s
	FROM %s
	WHERE state IN (%d, %d, %d, %d)
)`,
		queueCol, jobSetCol,
		jobTable,
		lookout.JobQueuedOrdinal, lookout.JobPendingOrdinal, lookout.JobRunningOrdinal, lookout.JobLeasedOrdinal,
	)
	joinWithActiveJobSetsTable = fmt.Sprintf("INNER JOIN %s AS %s USING (%s, %s)", activeJobSetsTable, activeJobSetsTableAbbrev, queueCol, jobSetCol)
)

type Query struct {
	Sql  string
	Args []interface{}
}

// QueryBuilder is a struct responsible for building a single Lookout SQL query
type QueryBuilder struct {
	// Returns information about database schema to create the queries
	lookoutTables *LookoutTables

	args []interface{}
}

// queryColumn contains all data related to a column to be used in a query
// The same column could be used in multiple databases, this struct will be used to determine which table should be used
// The abbreviation of the table, abbrev is included for ease
type queryColumn struct {
	name   string
	table  string
	abbrev string
}

func NewQueryBuilder(lookoutTables *LookoutTables) *QueryBuilder {
	return &QueryBuilder{
		lookoutTables: lookoutTables,
	}
}

func (qb *QueryBuilder) GetJobs(
	filters []*model.Filter,
	activeJobSets bool,
	order *model.Order,
	skip int,
	take int,
) (*Query, error) {
	if err := qb.validateFilters(filters); err != nil {
		return nil, errors.Wrap(err, "filters are invalid")
	}
	if err := qb.validateOrder(order); err != nil {
		return nil, errors.Wrap(err, "order is invalid")
	}

	activeJobSetsFilter := ""
	if activeJobSets {
		activeJobSetsFilter = joinWithActiveJobSetsTable
	}

	where, err := qb.makeWhere(filters)
	if err != nil {
		return nil, err
	}

	orderBy, err := qb.makeOrderBy(order)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(
		`SELECT
	selected_jobs.job_id,
	selected_jobs.queue,
	selected_jobs.owner,
	selected_jobs.namespace,
	selected_jobs.jobset,
	selected_jobs.cpu,
	selected_jobs.memory,
	selected_jobs.ephemeral_storage,
	selected_jobs.gpu,
	selected_jobs.priority,
	selected_jobs.submitted,
	selected_jobs.cancelled,
	selected_jobs.state,
	selected_jobs.last_transition_time,
	selected_jobs.duplicate,
	selected_jobs.priority_class,
	selected_jobs.latest_run_id,
	selected_jobs.cancel_reason,
	selected_jobs.cancel_user,
	selected_jobs.annotations,
	selected_runs.runs
FROM (
	SELECT *
	FROM %s AS %s
	%s
	%s
	%s
	%s
) AS selected_jobs
CROSS JOIN LATERAL (
	SELECT
		COALESCE(
			json_agg(
				json_strip_nulls(
					json_build_object(
						'runId', run_id,
						'cluster', cluster,
						'node', node,
						'leased', leased AT TIME ZONE 'UTC',
						'pending', pending AT TIME ZONE 'UTC',
						'started', started AT TIME ZONE 'UTC',
						'finished', finished AT TIME ZONE 'UTC',
						'jobRunState', job_run_state,
						'exitCode', exit_code
					)
				)
				ORDER BY COALESCE(leased, pending)
			) FILTER (WHERE run_id IS NOT NULL),
			'[]'
		) AS runs
	FROM %s
	WHERE job_id = selected_jobs.job_id
) AS selected_runs`,
		jobTable, jobTableAbbrev,
		activeJobSetsFilter,
		where,
		orderBy,
		limitOffsetSql(skip, take),
		jobRunTable,
	)

	return &Query{Sql: query, Args: qb.args}, nil
}

func (qb *QueryBuilder) GroupBy(
	filters []*model.Filter,
	activeJobSets bool,
	order *model.Order,
	groupedField *model.GroupedField,
	aggregates []string,
	skip int,
	take int,
) (*Query, error) {
	err := qb.validateFilters(filters)
	if err != nil {
		return nil, errors.Wrap(err, "filters are invalid")
	}
	err = qb.validateGroupOrder(order)
	if err != nil {
		return nil, errors.Wrap(err, "group order is invalid")
	}
	err = qb.validateAggregates(aggregates)
	if err != nil {
		return nil, errors.Wrap(err, "aggregates are invalid")
	}
	err = qb.validateGroupedField(groupedField)
	if err != nil {
		return nil, errors.Wrap(err, "group field is invalid")
	}

	activeJobSetsFilter := ""
	if activeJobSets {
		activeJobSetsFilter = joinWithActiveJobSetsTable
	}

	groupByColumn := queryColumn{table: jobTable, abbrev: jobTableAbbrev}
	if groupedField.IsAnnotation {
		groupByColumn.name = qb.annotationColumn(groupedField.Field)
	} else {
		groupByColumn.name = groupedField.Field
	}

	queryAggregators, err := qb.getQueryAggregators(aggregates, filters)
	if err != nil {
		return nil, err
	}
	selectList, err := qb.getAggregatesSql(queryAggregators)
	if err != nil {
		return nil, err
	}

	if groupedField.IsAnnotation {
		// scanGroup expects values in the grouped-by field not to be null. In
		// the case where we are grouping by an annotation key, we end up with
		// a `GROUP BY` clause like:
		//
		//     GROUP BY annotations->>'host_instance_id'
		//
		// This evaluates to null if the annotations object does not contain
		// the key in question, so we need to filter out such rows.
		filters = append(filters, &model.Filter{Field: groupedField.Field, Match: model.MatchExists, IsAnnotation: true})
	}
	where, err := qb.makeWhere(filters)
	if err != nil {
		return nil, err
	}

	groupBy, err := qb.createGroupBySQL(order, &groupByColumn, aggregates)
	if err != nil {
		return nil, err
	}

	orderBy, err := qb.groupByOrderSql(order)
	if err != nil {
		return nil, err
	}

	sql := fmt.Sprintf(
		`SELECT %s.%s, %s
FROM %s as %s
%s
%s
%s
%s
%s`,
		groupByColumn.abbrev, groupByColumn.name, selectList,
		jobTable, jobTableAbbrev,
		activeJobSetsFilter,
		where,
		groupBy,
		orderBy,
		limitOffsetSql(skip, take),
	)

	return &Query{Sql: sql, Args: qb.args}, nil
}

func (qb *QueryBuilder) createGroupBySQL(order *model.Order, groupCol *queryColumn, aggregates []string) (string, error) {
	expr := fmt.Sprintf("GROUP BY %s.%s", groupCol.abbrev, groupCol.name)
	isInAggregators := len(aggregates) > 0 && func(sl []string, t string) bool {
		for _, s := range sl {
			if s == t {
				return true
			}
		}
		return false
	}(aggregates, order.Field)
	if orderIsNull(order) || order.Field == countCol || isInAggregators {
		return expr, nil
	}
	col, err := qb.lookoutTables.ColumnFromField(order.Field)
	if err != nil {
		return expr, err
	}

	if groupCol.name == col {
		return expr, nil
	}
	// If order is not already grouped by or aggregated by, include it in GROUP BY statement
	return expr + fmt.Sprintf(", %s.%s", groupCol.abbrev, col), nil
}

func parseValueForState(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case string:
		ordinal, err := stateToOrdinal(v)
		if err != nil {
			return nil, err
		}
		return ordinal, nil
	case []string:
		result := make([]int, len(v))
		for i := 0; i < len(v); i++ {
			ordinal, err := stateToOrdinal(v[i])
			if err != nil {
				return nil, err
			}
			result[i] = ordinal
		}
		return result, nil
	case []interface{}:
		result := make([]int, len(v))
		for i := 0; i < len(v); i++ {
			str := fmt.Sprintf("%v", v[i])
			ordinal, err := stateToOrdinal(str)
			if err != nil {
				return nil, err
			}
			result[i] = ordinal
		}
		return result, nil
	default:
		return nil, errors.Errorf("unsupported type for state: %v: %T", value, value)
	}
}

func (qb *QueryBuilder) makeWhere(filters []*model.Filter) (string, error) {
	if len(filters) == 0 {
		return "", nil
	}
	var clauses []string
	for _, filter := range filters {
		clause, err := qb.makeWhereClause(filter)
		if err != nil {
			return "", err
		}
		clauses = append(clauses, clause)
	}
	return fmt.Sprintf("WHERE %s", strings.Join(clauses, " AND ")), nil
}

func (qb *QueryBuilder) makeWhereClause(filter *model.Filter) (string, error) {
	var column string
	if filter.IsAnnotation {
		switch filter.Match {
		case model.MatchExact:
			placeholder := qb.recordValue(map[string]interface{}{filter.Field: filter.Value})
			// GIN indexes are very particular about the kinds of predicates they
			// support; for example, neither
			//
			//     annotations->>'host_instance_id' = '35170439'
			//
			// nor
			//
			//     annotations['host_instance_id'] = '35170439'
			//
			// can use the GIN index on the annotations column, as jsonb_path_ops
			// GIN indexes only support the operators @>, @?, and @@:
			//
			//     https://www.postgresql.org/docs/current/datatype-json.html#JSON-INDEXING
			return fmt.Sprintf("%s.annotations @> %s", jobTableAbbrev, placeholder), nil
		case model.MatchExists:
			placeholder := qb.recordValue(filter.Field)
			return fmt.Sprintf("%s.annotations ? %s", jobTableAbbrev, placeholder), nil
		default:
			column = qb.annotationColumn(filter.Field)
		}
	} else {
		var err error
		column, err = qb.lookoutTables.ColumnFromField(filter.Field)
		if err != nil {
			return "", err
		}
	}

	operator, err := operatorForMatch(filter.Match)
	if err != nil {
		return "", err
	}

	value := filter.Value
	if column == stateCol {
		var err error
		value, err = parseValueForState(value)
		if err != nil {
			return "", err
		}
	}
	placeholder, err := qb.valueForMatch(value, filter.Match)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s.%s %s %s", jobTableAbbrev, column, operator, placeholder), nil
}

func (qb *QueryBuilder) annotationColumn(key string) string {
	placeholder := qb.recordValue(key)
	return fmt.Sprintf("annotations->>%s", placeholder)
}

func (qb *QueryBuilder) makeOrderBy(order *model.Order) (string, error) {
	if orderIsNull(order) {
		return "", nil
	}
	column, err := qb.lookoutTables.ColumnFromField(order.Field)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ORDER BY %s.%s %s", jobTableAbbrev, column, order.Direction), nil
}

func operatorForMatch(match string) (string, error) {
	switch match {
	case model.MatchExact:
		return "=", nil
	case model.MatchStartsWith:
		return "LIKE", nil
	case model.MatchContains:
		return "LIKE", nil
	case model.MatchAnyOf:
		return "IN", nil
	case model.MatchGreaterThan:
		return ">", nil
	case model.MatchLessThan:
		return "<", nil
	case model.MatchGreaterThanOrEqualTo:
		return ">=", nil
	case model.MatchLessThanOrEqualTo:
		return "<=", nil
	default:
		err := errors.Errorf("unsupported match type: %s", match)
		log.Error(err.Error())
		return "", err
	}
}

// Returns string to render in SQL, updates valuesMap with corresponding value(s)
func (qb *QueryBuilder) valueForMatch(value interface{}, match string) (string, error) {
	switch match {
	case model.MatchStartsWith:
		s := parseStringForLike(value)
		v := fmt.Sprintf("%s%%", s)
		return qb.recordValue(v), nil
	case model.MatchContains:
		s := parseStringForLike(value)
		v := fmt.Sprintf("%%%s%%", s)
		return qb.recordValue(v), nil
	case model.MatchAnyOf:
		switch v := value.(type) {
		case []interface{}:
			ids := make([]string, len(v))
			for i, val := range v {
				ids[i] = qb.recordValue(val)
			}
			return fmt.Sprintf("(%s)", strings.Join(ids, ", ")), nil
		case []int:
			ids := make([]string, len(v))
			for i, val := range v {
				ids[i] = qb.recordValue(val)
			}
			return fmt.Sprintf("(%s)", strings.Join(ids, ", ")), nil
		default:
			return "", errors.Errorf("unsupported type for anyOf: %T", v)
		}
	default:
		return qb.recordValue(value), nil
	}
}

func parseStringForLike(value interface{}) string {
	s := fmt.Sprintf("%v", value)
	return strings.ReplaceAll(s, "\\", "\\\\")
}

// Save value to be used in prepared statement, returns template string to put in place of the value in the SQL string
func (qb *QueryBuilder) recordValue(value interface{}) string {
	qb.args = append(qb.args, value)
	return fmt.Sprintf("$%d", len(qb.args))
}

func (qb *QueryBuilder) getQueryAggregators(aggregates []string, filters []*model.Filter) ([]QueryAggregator, error) {
	var queryAggregators []QueryAggregator
	for _, aggregate := range aggregates {
		col, err := qb.lookoutTables.ColumnFromField(aggregate)
		if err != nil {
			return nil, err
		}
		aggregateColumn := &queryColumn{
			name:   col,
			table:  jobTable,
			abbrev: jobTableAbbrev,
		}
		aggregateType, err := qb.lookoutTables.GroupAggregateForCol(col)
		if err != nil {
			return nil, err
		}
		newQueryAggregators, err := GetAggregatorsForColumn(aggregateColumn, aggregateType, filters)
		if err != nil {
			return nil, err
		}
		queryAggregators = append(queryAggregators, newQueryAggregators...)
	}
	return queryAggregators, nil
}

func (qb *QueryBuilder) getAggregatesSql(aggregators []QueryAggregator) (string, error) {
	selectList := []string{fmt.Sprintf("COUNT(*) AS %s", countCol)}
	for _, agg := range aggregators {
		sql, err := agg.AggregateSql()
		if err != nil {
			return "", err
		}
		selectList = append(selectList, sql)
	}
	return strings.Join(selectList, ", "), nil
}

func (qb *QueryBuilder) groupByOrderSql(order *model.Order) (string, error) {
	if orderIsNull(order) {
		return "", nil
	}
	if order.Field == countCol {
		return fmt.Sprintf("ORDER BY %s %s", countCol, order.Direction), nil
	}
	col, err := qb.lookoutTables.ColumnFromField(order.Field)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ORDER BY %s %s", col, order.Direction), nil
}

func limitOffsetSql(skip, take int) string {
	// Asking for zero rows is not useful to us, so we take a value of zero to
	// mean "no limit"; this is consistent with go-swagger, which uses zero as
	// the default value for optional integers:
	//
	//     https://github.com/go-swagger/go-swagger/issues/1707
	if take == 0 {
		return fmt.Sprintf("OFFSET %d", skip)
	}
	return fmt.Sprintf("LIMIT %d OFFSET %d", take, skip)
}

func (qb *QueryBuilder) validateFilters(filters []*model.Filter) error {
	for _, filter := range filters {
		err := qb.validateFilter(filter)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qb *QueryBuilder) validateFilter(filter *model.Filter) error {
	if filter.IsAnnotation {
		return validateAnnotationFilter(filter)
	}
	col, err := qb.lookoutTables.ColumnFromField(filter.Field)
	if err != nil {
		return err
	}
	if !qb.lookoutTables.IsFilterable(col) {
		return errors.Errorf("cannot filter by field %s", filter.Field)
	}
	if !qb.lookoutTables.SupportsMatch(col, filter.Match) {
		return errors.Errorf("match %s is not supported for field %s", filter.Match, filter.Field)
	}
	return nil
}

func validateAnnotationFilter(filter *model.Filter) error {
	if !slices.Contains([]string{
		model.MatchExact,
		model.MatchStartsWith,
		model.MatchContains,
		model.MatchExists,
	}, filter.Match) {
		return errors.Errorf("match %s is not supported for annotation", filter.Match)
	}
	return nil
}

func (qb *QueryBuilder) validateOrder(order *model.Order) error {
	if orderIsNull(order) {
		return nil
	}
	col, err := qb.lookoutTables.ColumnFromField(order.Field)
	if err != nil {
		return err
	}
	if !qb.lookoutTables.IsOrderable(col) {
		return errors.Errorf("cannot order by field %s", order.Field)
	}
	if !isValidOrderDirection(order.Direction) {
		return errors.Errorf("direction %s is not a valid sort direction", order.Direction)
	}
	return nil
}

func isValidOrderDirection(direction string) bool {
	return slices.Contains([]string{model.DirectionAsc, model.DirectionDesc}, direction)
}

func orderIsNull(order *model.Order) bool {
	return order == nil || (order.Direction == "" && order.Field == "")
}

func (qb *QueryBuilder) validateGroupOrder(order *model.Order) error {
	if order == nil {
		return nil
	}
	if order.Field == countCol {
		return nil
	}
	col, err := qb.lookoutTables.ColumnFromField(order.Field)
	if err != nil {
		return errors.Errorf("unsupported field for order: %s", order.Field)
	}

	_, err = qb.lookoutTables.GroupAggregateForCol(col)
	// If it is not an aggregate and not groupable, it can't be ordered by
	if err != nil && !qb.lookoutTables.IsGroupable(col) {
		return errors.Errorf("unsupported field for order: %s, cannot sort by column %s", order.Field, col)
	}
	return nil
}

func (qb *QueryBuilder) validateGroupedField(groupedField *model.GroupedField) error {
	if groupedField.IsAnnotation {
		// No check if is annotation
		return nil
	}
	col, err := qb.lookoutTables.ColumnFromField(groupedField.Field)
	if err != nil {
		return err
	}
	if !qb.lookoutTables.IsGroupable(col) {
		return errors.Errorf("cannot group by field %s", groupedField.Field)
	}
	return nil
}

func stateToOrdinal(state string) (int, error) {
	ordinal, ok := lookout.JobStateOrdinalMap[lookout.JobState(state)]
	if !ok {
		return -1, errors.Errorf("unknown state: %s", state)
	}
	return ordinal, nil
}

func (qb *QueryBuilder) validateAggregates(aggregates []string) error {
	for _, aggregate := range aggregates {
		col, err := qb.lookoutTables.ColumnFromField(aggregate)
		if err != nil {
			return err
		}
		_, err = qb.lookoutTables.GroupAggregateForCol(col)
		if err != nil {
			return err
		}
	}
	return nil
}
