package repository

import (
	"fmt"
	"github.com/G-Research/armada/internal/common/database/lookout"
	"math"
	"strings"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"k8s.io/utils/strings/slices"

	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookoutv2"
)

const countCol = "count"

type Query struct {
	Sql  string
	Args []interface{}
}

type QueryBuilder struct {
	lookoutTables *LookoutTables
}

type queryColumn struct {
	name   string
	table  string
	abbrev string
}

type queryFilter struct {
	column       *queryColumn
	value        interface{}
	match        string
	isAnnotation bool
}

type queryOrder struct {
	column    *queryColumn
	direction string
}

func (qb *QueryBuilder) CreateTempTable() (*Query, string) {
	tempTable := database.UniqueTableName(jobTable)
	sql := fmt.Sprintf(`
		CREATE TEMPORARY TABLE %[1]s (
			job_id varchar(32) NOT NULL
		) ON COMMIT DROP`, tempTable)
	return &Query{
		Sql:  sql,
		Args: []interface{}{},
	}, tempTable
}

func (qb *QueryBuilder) JobCount(filters []*lookoutv2.Filter) (*Query, error) {
	err := qb.validateFilters(filters)
	if err != nil {
		return &Query{}, errors.Wrap(err, "filters are invalid")
	}

	tablesFromColumns, err := qb.getAllTables(filters, &lookoutv2.Order{})
	if err != nil {
		return nil, err
	}
	queryTables, err := qb.determineTablesForQuery(tablesFromColumns)
	if err != nil {
		return nil, err
	}
	queryFilters, err := qb.makeQueryFilters(filters, queryTables)
	if err != nil {
		return nil, err
	}
	fromSql, err := qb.queryTablesToSql(queryTables, jobIdCol)
	if err != nil {
		return nil, err
	}
	valuesMap := make(map[string]interface{})
	whereSql, err := qb.queryFiltersToSql(queryFilters, valuesMap)
	if err != nil {
		return nil, err
	}
	abbrev, err := qb.firstTableAbbrev(queryTables)
	if err != nil {
		return nil, err
	}

	template := fmt.Sprintf(`
		SELECT COUNT(DISTINCT %s.job_id)
		%s
		%s`,
		abbrev, fromSql, whereSql)
	templated, args := templateSql(template, valuesMap)
	return &Query{
		Sql:  templated,
		Args: args,
	}, nil
}

func (qb *QueryBuilder) InsertIntoTempTable(tempTableName string, filters []*lookoutv2.Filter, order *lookoutv2.Order, skip, take int) (*Query, error) {
	err := qb.validateFilters(filters)
	if err != nil {
		return &Query{}, errors.Wrap(err, "filters are invalid")
	}
	err = qb.validateOrder(order)
	if err != nil {
		return &Query{}, errors.Wrap(err, "order is invalid")
	}

	tablesFromColumns, err := qb.getAllTables(filters, order)
	if err != nil {
		return nil, err
	}
	queryTables, err := qb.determineTablesForQuery(tablesFromColumns)
	if err != nil {
		return nil, err
	}
	queryFilters, err := qb.makeQueryFilters(filters, queryTables)
	if err != nil {
		return nil, err
	}
	queryOrd, err := qb.makeQueryOrder(order, queryTables)
	if err != nil {
		return nil, err
	}
	fromSql, err := qb.queryTablesToSql(queryTables, jobIdCol)
	if err != nil {
		return nil, err
	}
	valuesMap := make(map[string]interface{})
	whereSql, err := qb.queryFiltersToSql(queryFilters, valuesMap)
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

	templated, args := templateSql(template, valuesMap)
	return &Query{
		Sql:  templated,
		Args: args,
	}, nil
}

func (qb *QueryBuilder) CountGroups(filters []*lookoutv2.Filter, groupedField string) (*Query, error) {
	err := qb.validateFilters(filters)
	if err != nil {
		return &Query{}, errors.Wrap(err, "filters are invalid")
	}
	err = qb.validateGroupedField(groupedField)
	tablesFromColumns, err := qb.getAllTables(filters, &lookoutv2.Order{})
	if err != nil {
		return nil, err
	}
	queryTables, err := qb.determineTablesForQuery(tablesFromColumns)
	if err != nil {
		return nil, err
	}
	queryFilters, err := qb.makeQueryFilters(filters, queryTables)
	if err != nil {
		return nil, err
	}
	groupCol, err := qb.getQueryCol(groupedField, queryTables)
	if err != nil {
		return nil, err
	}
	fromSql, err := qb.queryTablesToSql(queryTables, jobIdCol)
	if err != nil {
		return nil, err
	}
	valuesMap := make(map[string]interface{})
	whereSql, err := qb.queryFiltersToSql(queryFilters, valuesMap)
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

	templated, args := templateSql(template, valuesMap)
	return &Query{
		Sql:  templated,
		Args: args,
	}, nil
}

func (qb *QueryBuilder) GroupBy(
	filters []*lookoutv2.Filter,
	order *lookoutv2.Order,
	groupedField string,
	skip int,
	take int,
) (*Query, error) {
	err := qb.validateFilters(filters)
	if err != nil {
		return &Query{}, errors.Wrap(err, "filters are invalid")
	}
	err = qb.validateGroupOrder(order)
	if err != nil {
		return &Query{}, errors.Wrap(err, "group order is invalid")
	}
	err = qb.validateGroupedField(groupedField)
	tablesFromColumns, err := qb.getAllTables(filters, &lookoutv2.Order{})
	if err != nil {
		return nil, err
	}
	queryTables, err := qb.determineTablesForQuery(tablesFromColumns)
	if err != nil {
		return nil, err
	}
	queryFilters, err := qb.makeQueryFilters(filters, queryTables)
	if err != nil {
		return nil, err
	}
	groupCol, err := qb.getQueryCol(groupedField, queryTables)
	if err != nil {
		return nil, err
	}
	fromSql, err := qb.queryTablesToSql(queryTables, jobIdCol)
	if err != nil {
		return nil, err
	}
	valuesMap := make(map[string]interface{})
	whereSql, err := qb.queryFiltersToSql(queryFilters, valuesMap)
	if err != nil {
		return nil, err
	}
	groupBySql := fmt.Sprintf("GROUP BY %s.%s", groupCol.abbrev, groupCol.name)
	orderSql := fmt.Sprintf("ORDER BY %s %s", order.Field, order.Direction)
	template := fmt.Sprintf(`
		SELECT %[1]s.%[2]s, COUNT(DISTINCT %[1]s.%[3]s) AS %[4]s
		%[5]s
		%[6]s
		%[7]s
		%[8]s
		%[9]s`,
		groupCol.abbrev, groupCol.name, jobIdCol, countCol, fromSql, whereSql, groupBySql, orderSql, limitOffsetSql(skip, take))
	templated, args := templateSql(template, valuesMap)
	fmt.Println(templated)
	fmt.Println(args)
	return &Query{
		Sql:  templated,
		Args: args,
	}, nil
}

// For each field/column, get the possible tables we could be querying as string set
func (qb *QueryBuilder) getAllTables(filters []*lookoutv2.Filter, order *lookoutv2.Order) ([]map[string]bool, error) {
	var result []map[string]bool
	for _, filter := range filters {
		if filter.IsAnnotation {
			result = append(result, util.StringListToSet([]string{userAnnotationLookupTable}))
			continue
		}
		tables, err := qb.tablesFromField(filter.Field)
		if err != nil {
			return nil, err
		}
		result = append(result, tables)
	}

	if !orderIsNull(order) {
		tables, err := qb.tablesFromField(order.Field)
		if err != nil {
			return nil, err
		}
		result = append(result, tables)
	}

	return result, nil
}

func (qb *QueryBuilder) tablesFromField(field string) (map[string]bool, error) {
	col, err := qb.lookoutTables.ColumnFromField(field)
	if err != nil {
		return nil, err
	}
	tables, err := qb.lookoutTables.TablesForColumn(col)
	if err != nil {
		return nil, err
	}
	return tables, nil
}

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
			for table, _ := range tablesForCol {
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
		for s, _ := range inter {
			if _, ok := sets[i][s]; ok {
				cur[s] = true
			}
		}
		inter = cur
	}
	return inter
}

func (qb *QueryBuilder) queryTablesToSql(queryTables map[string]bool, colToJoin string) (string, error) {
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
			table, abbrev, firstAbbrev, colToJoin))
	}
	return sb.String(), nil
}

// Get abbreviation for highest precedence table
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

func (qb *QueryBuilder) makeQueryFilters(filters []*lookoutv2.Filter, queryTables map[string]bool) ([]*queryFilter, error) {
	result := make([]*queryFilter, len(filters))
	for i, filter := range filters {
		if filter.IsAnnotation {
			result[i] = &queryFilter{
				column: &queryColumn{
					name:   filter.Field,
					table:  userAnnotationLookupTable,
					abbrev: userAnnotationLookupTableAbbrev,
				},
				value:        filter.Value,
				match:        filter.Match,
				isAnnotation: true,
			}
			continue
		}
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
			default:
				return nil, errors.Errorf("unsupported type for state: %v", filter.Value)
			}
		}
		result[i] = &queryFilter{
			column: &queryColumn{
				name:   col,
				table:  table,
				abbrev: abbrev,
			},
			value:        value,
			match:        filter.Match,
			isAnnotation: filter.IsAnnotation,
		}
	}
	return result, nil
}

func (qb *QueryBuilder) queryFiltersToSql(filters []*queryFilter, valuesMap map[string]interface{}) (string, error) {
	if len(filters) == 0 {
		return "", nil
	}
	var exprs []string
	for _, filter := range filters {
		if filter.isAnnotation {
			// Need two filters, one for annotation key, one for annotation value
			keyExpr, err := qb.comparisonExpr(filter.column.name, lookoutv2.MatchExact, filter.column.abbrev, annotationKeyCol, valuesMap)
			if err != nil {
				return "", err
			}
			valueExpr, err := qb.comparisonExpr(filter.value, filter.match, filter.column.abbrev, annotationValueCol, valuesMap)
			exprs = append(exprs, keyExpr, valueExpr)
			continue
		}
		expr, err := qb.comparisonExpr(filter.value, filter.match, filter.column.abbrev, filter.column.name, valuesMap)
		if err != nil {
			return "", err
		}
		exprs = append(exprs, expr)
	}
	return fmt.Sprintf("WHERE %s", strings.Join(exprs, " AND ")), nil
}

func (qb *QueryBuilder) comparisonExpr(value interface{}, match, abbrev, colName string, valuesMap map[string]interface{}) (string, error) {
	comparator, err := qb.comparatorForMatch(match)
	if err != nil {
		return "", err
	}
	formattedValue, err := qb.valueForMatch(value, match, valuesMap)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"%s.%s %s %s",
		abbrev, colName, comparator, formattedValue), nil
}

func (qb *QueryBuilder) comparatorForMatch(match string) (string, error) {
	switch match {
	case lookoutv2.MatchExact:
		return "=", nil
	case lookoutv2.MatchStartsWith:
		return "LIKE", nil
	case lookoutv2.MatchAnyOf:
		return "IN", nil
	default:
		err := errors.Errorf("unsupported match type: %s", match)
		logrus.Error(err)
		return "", err
	}
}

// Returns string to render in SQL, updates valuesMap with corresponding value(s)
func (qb *QueryBuilder) valueForMatch(value interface{}, match string, valuesMap map[string]interface{}) (string, error) {
	switch match {
	case lookoutv2.MatchStartsWith:
		v := fmt.Sprintf("%v%%", value)
		id := uuid.NewString()
		valuesMap[id] = v
		return idToTemplateString(id), nil
	case lookoutv2.MatchAnyOf:
		switch v := value.(type) {
		case []int:
			ids := make([]string, len(v))
			for i, val := range v {
				id := uuid.NewString()
				valuesMap[id] = val
				ids[i] = idToTemplateString(id)
			}
			return fmt.Sprintf("(%s)", strings.Join(ids, ", ")), nil
		default:
			return "", errors.Errorf("unsupported type for anyOf: %T", v)
		}
	default:
		id := uuid.NewString()
		valuesMap[id] = value
		return idToTemplateString(id), nil
	}
}

func (qb *QueryBuilder) makeQueryOrder(order *lookoutv2.Order, queryTables map[string]bool) (*queryOrder, error) {
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

func (qb *QueryBuilder) queryOrderToSql(order *queryOrder) string {
	if order == nil {
		return ""
	}
	return fmt.Sprintf("ORDER BY %s.%s %s", order.column.abbrev, order.column.name, order.direction)
}

func (qb *QueryBuilder) getQueryCol(groupedField string, queryTables map[string]bool) (*queryColumn, error) {
	col, err := qb.lookoutTables.ColumnFromField(groupedField)
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
	return &queryColumn{
		name:   col,
		table:  table,
		abbrev: abbrev,
	}, nil
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

func limitOffsetSql(skip, take int) string {
	return fmt.Sprintf("LIMIT %d OFFSET %d", take, skip)
}

func (qb *QueryBuilder) validateFilters(filters []*lookoutv2.Filter) error {
	for _, filter := range filters {
		err := qb.validateFilter(filter)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qb *QueryBuilder) validateFilter(filter *lookoutv2.Filter) error {
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

func validateAnnotationFilter(filter *lookoutv2.Filter) error {
	if !slices.Contains([]string{lookoutv2.MatchExact, lookoutv2.MatchStartsWith, lookoutv2.MatchContains}, filter.Match) {
		return errors.Errorf("match %s is not supported for annotation", filter.Match)
	}
	return nil
}

func (qb *QueryBuilder) validateOrder(order *lookoutv2.Order) error {
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
	return slices.Contains([]string{lookoutv2.DirectionAsc, lookoutv2.DirectionDesc}, direction)
}

func orderIsNull(order *lookoutv2.Order) bool {
	return order == nil || (order.Direction == "" && order.Field == "")
}

func (qb *QueryBuilder) validateGroupOrder(order *lookoutv2.Order) error {
	if order.Field != countCol {
		return errors.Errorf("unsupported group ordering: %s", order.Field)
	}
	return nil
}

func (qb *QueryBuilder) validateGroupedField(groupedField string) error {
	if !qb.lookoutTables.IsGroupable(groupedField) {
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
