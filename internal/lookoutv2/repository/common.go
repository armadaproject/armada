package repository

import (
	"fmt"
	"math"
	"strings"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"k8s.io/utils/strings/slices"

	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
)

const (
	countCol                    = "count"
	annotationFilterTableAbbrev = "aft"
)

type Query struct {
	Sql  string
	Args []interface{}
}

// QueryBuilder is a struct responsible for building a single Lookout SQL query
type QueryBuilder struct {
	// Returns information about database schema to create the queries
	lookoutTables *LookoutTables
	// Mapping from UUID to value to be used in query - we will use this mapping to create a prepared SQL query,
	// substituting each UUID in the SQL string with $1, $2, ...
	queryValues map[string]interface{}
}

// queryColumn contains all data related to a column to be used in a query
// The same column could be used in multiple databases, this struct will be used to determine which table should be used
// The abbreviation of the table, abbrev is included for ease
type queryColumn struct {
	name   string
	table  string
	abbrev string
}

// Represents data required to construct a condition based on a column, it's desired value, and match expression
type queryFilter struct {
	column *queryColumn
	value  interface{}
	match  string
}

// Represents data required to construct a sort expression based on a column and a direction.
// The direction can be "ASC" or "DESC"
type queryOrder struct {
	column    *queryColumn
	direction string
}

// Get aggregation expression for column, e.g. MAX(j.submitted)
type aggregatorFn func(column *queryColumn) string

type queryAggregator struct {
	column     *queryColumn
	aggregator aggregatorFn
}

func NewQueryBuilder(lookoutTables *LookoutTables) *QueryBuilder {
	return &QueryBuilder{
		lookoutTables: lookoutTables,
		queryValues:   make(map[string]interface{}),
	}
}

// CreateTempTable creates a temporary table of job ids
// Returns the Query and the name of the temporary table, to be used later in InsertIntoTempTable
func (qb *QueryBuilder) CreateTempTable() (*Query, string) {
	tempTable := database.UniqueTableName(jobTable)
	sql := fmt.Sprintf(`
		CREATE TEMPORARY TABLE %s (
			job_id varchar(32) NOT NULL
		) ON COMMIT DROP`, tempTable)
	return &Query{
		Sql:  sql,
		Args: []interface{}{},
	}, tempTable
}

// JobCount Returns SQL Query that when executed will return the total number of jobs that match the list of filters
func (qb *QueryBuilder) JobCount(filters []*model.Filter) (*Query, error) {
	err := qb.validateFilters(filters)
	if err != nil {
		return nil, errors.Wrap(err, "filters are invalid")
	}
	normalFilters, annotationFilters := splitFilters(filters)

	allCols, err := qb.fieldsToCols(
		util.Map(normalFilters, func(filter *model.Filter) string { return filter.Field }),
	)
	if err != nil {
		return nil, err
	}
	tablesFromColumns, err := qb.tablesForCols(allCols)
	if err != nil {
		return nil, err
	}
	queryTables, err := qb.determineTablesForQuery(tablesFromColumns)
	if err != nil {
		return nil, err
	}
	queryFilters, err := qb.makeQueryFilters(normalFilters, queryTables)
	if err != nil {
		return nil, err
	}
	fromSql, err := qb.makeFromSql(queryTables, annotationFilters)
	if err != nil {
		return nil, err
	}
	whereSql, err := qb.queryFiltersToSql(queryFilters)
	if err != nil {
		return nil, err
	}
	abbrev, err := qb.firstTableAbbrev(queryTables)
	if err != nil {
		return nil, err
	}

	countExpr := fmt.Sprintf("COUNT(DISTINCT %s.job_id)", abbrev)
	// If we are only fetching from jobs, no need to count distinct, as it is a big performance hit
	if _, ok := queryTables[jobTable]; ok && len(queryTables) == 1 && len(annotationFilters) == 0 {
		countExpr = "COUNT(*)"
	}
	template := fmt.Sprintf(`
		SELECT %s
		%s
		%s`,
		countExpr, fromSql, whereSql)
	templated, args := templateSql(template, qb.queryValues)
	return &Query{
		Sql:  templated,
		Args: args,
	}, nil
}

// InsertIntoTempTable returns Query that returns Job IDs according to filters, order, skip and take, and inserts them
// in the temp table with name tempTableName
func (qb *QueryBuilder) InsertIntoTempTable(tempTableName string, filters []*model.Filter, order *model.Order, skip, take int) (*Query, error) {
	err := qb.validateFilters(filters)
	if err != nil {
		return nil, errors.Wrap(err, "filters are invalid")
	}
	err = qb.validateOrder(order)
	if err != nil {
		return nil, errors.Wrap(err, "order is invalid")
	}
	normalFilters, annotationFilters := splitFilters(filters)

	fields := util.Map(normalFilters, func(filter *model.Filter) string { return filter.Field })
	if !orderIsNull(order) {
		fields = append(fields, order.Field)
	}
	allCols, err := qb.fieldsToCols(fields)
	if err != nil {
		return nil, err
	}
	tablesFromColumns, err := qb.tablesForCols(allCols)
	if err != nil {
		return nil, err
	}
	queryTables, err := qb.determineTablesForQuery(tablesFromColumns)
	if err != nil {
		return nil, err
	}
	queryFilters, err := qb.makeQueryFilters(normalFilters, queryTables)
	if err != nil {
		return nil, err
	}
	queryOrd, err := qb.makeQueryOrder(order, queryTables)
	if err != nil {
		return nil, err
	}
	fromSql, err := qb.makeFromSql(queryTables, annotationFilters)
	if err != nil {
		return nil, err
	}
	whereSql, err := qb.queryFiltersToSql(queryFilters)
	if err != nil {
		return nil, err
	}
	orderSql := qb.queryOrderToSql(queryOrd)
	abbrev, err := qb.firstTableAbbrev(queryTables)
	if err != nil {
		return nil, err
	}
	template := fmt.Sprintf(`
		INSERT INTO %s (job_id)
		SELECT %s.job_id
		%s
		%s
		%s
		%s
		ON CONFLICT DO NOTHING`,
		tempTableName, abbrev, fromSql, whereSql, orderSql, limitOffsetSql(skip, take))

	templated, args := templateSql(template, qb.queryValues)
	return &Query{
		Sql:  templated,
		Args: args,
	}, nil
}

// CountGroups returns Query that counts the total number of groups created by grouping by groupedField and filtering
// with filters
func (qb *QueryBuilder) CountGroups(filters []*model.Filter, groupedField string) (*Query, error) {
	err := qb.validateFilters(filters)
	if err != nil {
		return nil, errors.Wrap(err, "filters are invalid")
	}
	err = qb.validateGroupedField(groupedField)
	if err != nil {
		return nil, err
	}
	normalFilters, annotationFilters := splitFilters(filters)

	allCols, err := qb.fieldsToCols(
		util.Map(normalFilters, func(filter *model.Filter) string { return filter.Field }),
	)
	if err != nil {
		return nil, err
	}
	tablesFromColumns, err := qb.tablesForCols(allCols)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	queryTables, err := qb.determineTablesForQuery(tablesFromColumns)
	if err != nil {
		return nil, err
	}
	queryFilters, err := qb.makeQueryFilters(normalFilters, queryTables)
	if err != nil {
		return nil, err
	}
	groupCol, err := qb.getGroupByQueryCol(groupedField, queryTables)
	if err != nil {
		return nil, err
	}
	fromSql, err := qb.makeFromSql(queryTables, annotationFilters)
	if err != nil {
		return nil, err
	}
	whereSql, err := qb.queryFiltersToSql(queryFilters)
	if err != nil {
		return nil, err
	}
	groupBySql := fmt.Sprintf("GROUP BY %s.%s", groupCol.abbrev, groupCol.name)
	template := fmt.Sprintf(`
		SELECT COUNT(*) FROM (
			SELECT %s.%s
			%s
			%s
			%s
		) AS group_table`,
		groupCol.abbrev, groupCol.name, fromSql, whereSql, groupBySql)

	templated, args := templateSql(template, qb.queryValues)
	return &Query{
		Sql:  templated,
		Args: args,
	}, nil
}

// GroupBy returns Query that performs a group by on filters
func (qb *QueryBuilder) GroupBy(
	filters []*model.Filter,
	order *model.Order,
	groupedField string,
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
	err = qb.validateGroupedField(groupedField)
	if err != nil {
		return nil, errors.Wrap(err, "group field is invalid")
	}
	err = qb.validateAggregates(aggregates)
	if err != nil {
		return nil, errors.Wrap(err, "aggregates are invalid")
	}

	normalFilters, annotationFilters := splitFilters(filters)
	fields := util.Concat(
		util.Map(normalFilters, func(filter *model.Filter) string { return filter.Field }),
		aggregates,
	)
	if !orderIsNull(order) && order.Field != countCol { // count does not correspond to a column in any table
		fields = append(fields, order.Field)
	}
	allCols, err := qb.fieldsToCols(fields)
	if err != nil {
		return nil, err
	}
	tablesFromColumns, err := qb.tablesForCols(allCols)
	if err != nil {
		return nil, err
	}
	queryTables, err := qb.determineTablesForQuery(tablesFromColumns)
	if err != nil {
		return nil, err
	}
	queryFilters, err := qb.makeQueryFilters(normalFilters, queryTables)
	if err != nil {
		return nil, err
	}
	groupCol, err := qb.getGroupByQueryCol(groupedField, queryTables)
	if err != nil {
		return nil, err
	}
	fromSql, err := qb.makeFromSql(queryTables, annotationFilters)
	if err != nil {
		return nil, err
	}
	whereSql, err := qb.queryFiltersToSql(queryFilters)
	if err != nil {
		return nil, err
	}
	groupBySql := fmt.Sprintf("GROUP BY %s.%s", groupCol.abbrev, groupCol.name)
	queryAggregators, err := qb.getQueryAggregators(aggregates, queryTables)
	if err != nil {
		return nil, err
	}
	selectListSql := qb.getAggregatesSql(queryAggregators)
	orderSql, err := qb.groupByOrderSql(order)
	if err != nil {
		return nil, err
	}
	template := fmt.Sprintf(`
		SELECT %[1]s.%[2]s, %[3]s
		%[4]s
		%[5]s
		%[6]s
		%[7]s
		%[8]s`,
		groupCol.abbrev, groupCol.name, selectListSql, fromSql, whereSql, groupBySql, orderSql, limitOffsetSql(skip, take))
	templated, args := templateSql(template, qb.queryValues)
	return &Query{
		Sql:  templated,
		Args: args,
	}, nil
}

func (qb *QueryBuilder) fieldsToCols(fields []string) ([]string, error) {
	var cols []string
	for _, field := range fields {
		col, err := qb.lookoutTables.ColumnFromField(field)
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, nil
}

// For each column, get all the possible tables we could be querying
// returns a list of string sets, one set of tables per column
func (qb *QueryBuilder) tablesForCols(cols []string) ([]map[string]bool, error) {
	var result []map[string]bool
	for _, col := range cols {
		tables, err := qb.lookoutTables.TablesForColumn(col)
		if err != nil {
			return nil, err
		}
		result = append(result, tables)
	}
	return result, nil
}

// For each query, we will have to query one or more columns. Each column can be found in one or more tables.
// To optimise queries, we want to find the smallest set of tables that includes all columns required in the query.
// determineTablesForQuery takes a list of sets of tables (one set of tables for each column), and returns the minimal
// set of tables that includes all columns.
// E.g. three tables: A, B, C
//
//	Col 1 is in table [A, B]
//	Col 2 is in table [B]
//	Col 3 is in table [B]
//	Col 4 is in table [C]
//	Therefore, the smallest set of tables to use is [B, C]
//
// If multiple tables can be used, it picks the one with the highest precedence
func (qb *QueryBuilder) determineTablesForQuery(tablesForColumns []map[string]bool) (map[string]bool, error) {
	if len(tablesForColumns) == 0 {
		return util.StringListToSet([]string{jobTable}), nil
	}
	inter := intersection(tablesForColumns)
	if len(inter) > 0 {
		for _, table := range qb.lookoutTables.TablePrecedence() {
			_, ok := inter[table]
			if ok {
				return util.StringListToSet([]string{table}), nil
			}
		}
	}

	// Compute power set of tables, and select smallest set that includes all columns
	nTables := len(qb.lookoutTables.TablePrecedence())
	nSets := int(math.Pow(2, float64(nTables))) - 1
	i := 1
	bestSet := map[string]bool{}
	for i <= nSets {
		mask := i
		j := 0
		set := map[string]bool{}
		for mask > 0 {
			maybeOne := mask & 1
			if maybeOne == 1 {
				set[qb.lookoutTables.TablePrecedence()[j]] = true
			}
			mask = mask >> 1
			j += 1
		}

		didMatch := true
		for _, tablesForCol := range tablesForColumns {
			didMatchCol := false
			for table := range tablesForCol {
				if _, ok := set[table]; ok {
					didMatchCol = true
				}
			}
			if !didMatchCol {
				didMatch = false
				break
			}
		}

		if didMatch && (len(bestSet) == 0 || len(bestSet) > len(set)) {
			bestSet = set
		}
		i++
	}

	return bestSet, nil
}

// Takes list of sets and returns their intersection
func intersection(sets []map[string]bool) map[string]bool {
	if len(sets) == 0 {
		return map[string]bool{}
	}
	inter := sets[0]
	for i := 1; i < len(sets); i++ {
		cur := make(map[string]bool)
		for s := range inter {
			if _, ok := sets[i][s]; ok {
				cur[s] = true
			}
		}
		inter = cur
	}
	return inter
}

// Split filters into those for normal columns and those for annotations
func splitFilters(filters []*model.Filter) ([]*model.Filter, []*model.Filter) {
	var normalFilters []*model.Filter
	var annotationFilters []*model.Filter
	for _, filter := range filters {
		if filter.IsAnnotation {
			annotationFilters = append(annotationFilters, filter)
		} else {
			normalFilters = append(normalFilters, filter)
		}
	}
	return normalFilters, annotationFilters
}

// makeFromSql creates FROM clause using a set of tables,
// joining them on colToJoin if multiple tables are present
// If annotations filters are present, inner joins on a table to select matching job ids with all the annotations
func (qb *QueryBuilder) makeFromSql(queryTables map[string]bool, annotationFilters []*model.Filter) (string, error) {
	sortedTables := make([]string, len(queryTables))
	idx := 0
	for _, table := range qb.lookoutTables.TablePrecedence() {
		if _, ok := queryTables[table]; !ok {
			continue
		}
		sortedTables[idx] = table
		idx++
	}
	sb := strings.Builder{}
	firstAbbrev, err := qb.lookoutTables.TableAbbrev(sortedTables[0])
	if err != nil {
		return "", err
	}
	sb.WriteString(fmt.Sprintf("FROM %s AS %s", sortedTables[0], firstAbbrev))
	for i := 1; i < len(sortedTables); i++ {
		table := sortedTables[i]
		abbrev, err := qb.lookoutTables.TableAbbrev(table)
		if err != nil {
			return "", err
		}
		sb.WriteString(fmt.Sprintf(
			" LEFT JOIN %[1]s AS %[2]s ON %[3]s.%[4]s = %[2]s.%[4]s",
			table, abbrev, firstAbbrev, jobIdCol))
	}
	if len(annotationFilters) > 0 {
		annotationFilterTableSql, err := qb.createAnnotationFilterTableSql(annotationFilters)
		if err != nil {
			return "", err
		}
		sb.WriteString(fmt.Sprintf(" INNER JOIN (%[1]s) AS %[2]s ON %[3]s.%[4]s = %[2]s.%[4]s",
			annotationFilterTableSql,
			annotationFilterTableAbbrev,
			firstAbbrev,
			jobIdCol))
	}
	return sb.String(), nil
}

func (qb *QueryBuilder) createAnnotationFilterTableSql(annotationFilters []*model.Filter) (string, error) {
	whereExprs := make([]string, len(annotationFilters))
	for i, filter := range annotationFilters {
		comparator, err := qb.comparatorForMatch(filter.Match)
		if err != nil {
			return "", err
		}
		key, err := qb.valueForMatch(filter.Field, model.MatchExact)
		if err != nil {
			return "", err
		}
		value, err := qb.valueForMatch(filter.Value, filter.Match)
		if err != nil {
			return "", err
		}
		whereExprs[i] = fmt.Sprintf("%s = %s AND %s %s %s", annotationKeyCol, key, annotationValueCol, comparator, value)
	}
	joinedWhere := strings.Join(whereExprs, ") OR (")
	return fmt.Sprintf(`
		SELECT %[1]s
		FROM %[2]s
		WHERE (%[3]s)
		GROUP BY %[1]s
		HAVING COUNT(*) = %[4]d
	`, jobIdCol, userAnnotationLookupTable, joinedWhere, len(annotationFilters)), nil
}

// Get abbreviation for highest precedence table out of a set of tables
func (qb *QueryBuilder) firstTableAbbrev(queryTables map[string]bool) (string, error) {
	for _, table := range qb.lookoutTables.TablePrecedence() {
		if _, ok := queryTables[table]; ok {
			abbrev, err := qb.lookoutTables.TableAbbrev(table)
			if err != nil {
				return "", err
			}
			return abbrev, nil
		}
	}
	return "", errors.New("no tables")
}

// makeQueryFilters takes a list of external filters and a set of tables to perform the queries on, and returns the
// corresponding list of queryFilters which will be used to generate the WHERE clause for the query
func (qb *QueryBuilder) makeQueryFilters(filters []*model.Filter, queryTables map[string]bool) ([]*queryFilter, error) {
	result := make([]*queryFilter, len(filters))
	for i, filter := range filters {
		col, err := qb.lookoutTables.ColumnFromField(filter.Field)
		if err != nil {
			return nil, err
		}
		table, err := qb.highestPrecedenceTableForColumn(col, queryTables)
		if err != nil {
			return nil, err
		}
		abbrev, err := qb.lookoutTables.TableAbbrev(table)
		if err != nil {
			return nil, err
		}
		value := filter.Value
		if col == stateCol {
			switch v := filter.Value.(type) {
			case string:
				ordinal, err := stateToOrdinal(v)
				if err != nil {
					return nil, err
				}
				value = ordinal
			case []string:
				result := make([]int, len(v))
				for i := 0; i < len(v); i++ {
					ordinal, err := stateToOrdinal(v[i])
					if err != nil {
						return nil, err
					}
					result[i] = ordinal
				}
				value = result
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
				value = result
			default:
				return nil, errors.Errorf("unsupported type for state: %v: %T", filter.Value, filter.Value)
			}
		}
		result[i] = &queryFilter{
			column: &queryColumn{
				name:   col,
				table:  table,
				abbrev: abbrev,
			},
			value: value,
			match: filter.Match,
		}
	}
	return result, nil
}

// queryFiltersToSql converts list of queryFilters to WHERE clause
func (qb *QueryBuilder) queryFiltersToSql(filters []*queryFilter) (string, error) {
	if len(filters) == 0 {
		return "", nil
	}
	var exprs []string
	for _, filter := range filters {
		expr, err := qb.comparisonExpr(filter.value, filter.match, filter.column.abbrev, filter.column.name)
		if err != nil {
			return "", err
		}
		exprs = append(exprs, expr)
	}
	return fmt.Sprintf("WHERE %s", strings.Join(exprs, " AND ")), nil
}

// Given a value, a match, a table abbreviation and a column name, returns the corresponding comparison expression for
// use in a WHERE clause
func (qb *QueryBuilder) comparisonExpr(value interface{}, match, abbrev, colName string) (string, error) {
	comparator, err := qb.comparatorForMatch(match)
	if err != nil {
		return "", err
	}
	formattedValue, err := qb.valueForMatch(value, match)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"%s.%s %s %s",
		abbrev, colName, comparator, formattedValue), nil
}

// Given a match string, return the corresponding SQL compare operation
func (qb *QueryBuilder) comparatorForMatch(match string) (string, error) {
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
		logrus.Error(err)
		return "", err
	}
}

// Returns string to render in SQL, updates valuesMap with corresponding value(s)
func (qb *QueryBuilder) valueForMatch(value interface{}, match string) (string, error) {
	switch match {
	case model.MatchStartsWith:
		v := fmt.Sprintf("%v%%", value)
		return qb.recordValue(v), nil
	case model.MatchContains:
		v := fmt.Sprintf("%%%v%%", value)
		return qb.recordValue(v), nil
	case model.MatchAnyOf:
		switch v := value.(type) {
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

// Save value to be used in prepared statement, returns template string to put in place of the value in the SQL string
func (qb *QueryBuilder) recordValue(value interface{}) string {
	id := uuid.NewString()
	qb.queryValues[id] = value
	return idToTemplateString(id)
}

// makeQueryOrder takes an external order and a set of tables to perform the queries on, and returns the
// corresponding queryOrder which will be used to generate the ORDER BY clause for the query
func (qb *QueryBuilder) makeQueryOrder(order *model.Order, queryTables map[string]bool) (*queryOrder, error) {
	if orderIsNull(order) {
		return nil, nil
	}
	col, err := qb.lookoutTables.ColumnFromField(order.Field)
	if err != nil {
		return nil, err
	}
	table, err := qb.highestPrecedenceTableForColumn(col, queryTables)
	if err != nil {
		return nil, err
	}
	abbrev, err := qb.lookoutTables.TableAbbrev(table)
	if err != nil {
		return nil, err
	}
	return &queryOrder{
		column: &queryColumn{
			name:   col,
			table:  table,
			abbrev: abbrev,
		},
		direction: order.Direction,
	}, nil
}

// queryOrderToSql converts list of queryFilters to WHERE clause
func (qb *QueryBuilder) queryOrderToSql(order *queryOrder) string {
	if order == nil {
		return ""
	}
	return fmt.Sprintf("ORDER BY %s.%s %s", order.column.abbrev, order.column.name, order.direction)
}

// getGroupByQueryCol finds the groupedField's corresponding column and best table to group by on
func (qb *QueryBuilder) getGroupByQueryCol(groupedField string, queryTables map[string]bool) (*queryColumn, error) {
	col, err := qb.lookoutTables.ColumnFromField(groupedField)
	if err != nil {
		return nil, err
	}
	return qb.getQueryColumn(col, queryTables)
}

// Gets the highest precedence table for a given column, among the tables that have already been selected for the query
func (qb *QueryBuilder) highestPrecedenceTableForColumn(col string, queryTables map[string]bool) (string, error) {
	colTables, err := qb.lookoutTables.TablesForColumn(col)
	if err != nil {
		return "", err
	}
	var selectedTable string
	for _, table := range qb.lookoutTables.TablePrecedence() {
		_, isInQueryTables := queryTables[table]
		_, isInColTables := colTables[table]
		if isInQueryTables && isInColTables {
			selectedTable = table
			break
		}
	}
	if selectedTable == "" {
		return "", errors.New("no table found")
	}
	return selectedTable, nil
}

func (qb *QueryBuilder) getQueryAggregators(aggregates []string, queryTables map[string]bool) ([]*queryAggregator, error) {
	queryAggregators := make([]*queryAggregator, len(aggregates))
	for i, aggregate := range aggregates {
		col, err := qb.lookoutTables.ColumnFromField(aggregate)
		if err != nil {
			return nil, err
		}
		qc, err := qb.getQueryColumn(col, queryTables)
		if err != nil {
			return nil, err
		}
		aggregateType, err := qb.lookoutTables.GroupAggregateForCol(col)
		if err != nil {
			return nil, err
		}
		fn, err := getAggregatorFn(aggregateType)
		if err != nil {
			return nil, err
		}
		queryAggregators[i] = &queryAggregator{
			column:     qc,
			aggregator: fn,
		}
	}
	return queryAggregators, nil
}

func (qb *QueryBuilder) getAggregatesSql(aggregators []*queryAggregator) string {
	selectList := []string{"COUNT(*) AS count"}
	for _, agg := range aggregators {
		sql := fmt.Sprintf("%s AS %s", agg.aggregator(agg.column), agg.column.name)
		selectList = append(selectList, sql)
	}
	return strings.Join(selectList, ", ")
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

func getAggregatorFn(aggregateType AggregateType) (aggregatorFn, error) {
	switch aggregateType {
	case Max:
		return func(col *queryColumn) string {
			return fmt.Sprintf("MAX(%s.%s)", col.abbrev, col.name)
		}, nil
	case Average:
		return func(col *queryColumn) string {
			return fmt.Sprintf("AVG(%s.%s)", col.abbrev, col.name)
		}, nil
	case Unknown:
		return nil, errors.New("unknown aggregate type")
	default:
		return nil, errors.Errorf("cannot determine aggregate type: %v", aggregateType)
	}
}

func (qb *QueryBuilder) getQueryColumn(col string, queryTables map[string]bool) (*queryColumn, error) {
	table, err := qb.highestPrecedenceTableForColumn(col, queryTables)
	if err != nil {
		return nil, err
	}
	abbrev, err := qb.lookoutTables.TableAbbrev(table)
	if err != nil {
		return nil, err
	}
	return &queryColumn{
		name:   col,
		table:  table,
		abbrev: abbrev,
	}, nil
}

func limitOffsetSql(skip, take int) string {
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
	if !slices.Contains([]string{model.MatchExact, model.MatchStartsWith, model.MatchContains}, filter.Match) {
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
	if err != nil {
		return errors.Errorf("unsupported field for order: %s, cannot sort by column %s", order.Field, col)
	}
	return nil
}

func (qb *QueryBuilder) validateGroupedField(groupedField string) error {
	col, err := qb.lookoutTables.ColumnFromField(groupedField)
	if err != nil {
		return err
	}
	if !qb.lookoutTables.IsGroupable(col) {
		return errors.Errorf("cannot group by field %s", groupedField)
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
