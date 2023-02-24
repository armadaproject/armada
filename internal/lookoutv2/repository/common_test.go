package repository

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
)

var testFilters = []*model.Filter{
	{
		Field: "queue",
		Match: "exact",
		Value: "test-queue",
	},
	{
		Field: "owner",
		Match: "startsWith",
		Value: "anon",
	},
	{
		Field:        "1234",
		Match:        "exact",
		Value:        "abcd",
		IsAnnotation: true,
	},
	{
		Field:        "5678",
		Match:        "startsWith",
		Value:        "efgh",
		IsAnnotation: true,
	},
}

func TestIntersection(t *testing.T) {
	t.Run("no items", func(t *testing.T) {
		result := intersection([]map[string]bool{})
		assert.Equal(t, map[string]bool{}, result)
	})
	t.Run("single item in intersection", func(t *testing.T) {
		result := intersection([]map[string]bool{
			util.StringListToSet([]string{"a", "b"}),
			util.StringListToSet([]string{"b", "c"}),
		})
		assert.Equal(t, map[string]bool{"b": true}, result)
	})
	t.Run("no items in intersection", func(t *testing.T) {
		result := intersection([]map[string]bool{
			util.StringListToSet([]string{"a", "b"}),
			util.StringListToSet([]string{"c", "d"}),
		})
		assert.Equal(t, map[string]bool{}, result)
	})
	t.Run("multiple items in intersection", func(t *testing.T) {
		result := intersection([]map[string]bool{
			util.StringListToSet([]string{"a", "b"}),
			util.StringListToSet([]string{"a", "b", "d"}),
			util.StringListToSet([]string{"a", "b", "d", "x", "y"}),
		})
		assert.Equal(t, map[string]bool{"a": true, "b": true}, result)
	})
}

func TestQueryBuilder_DetermineTablesForQuery(t *testing.T) {
	qb := QueryBuilder{lookoutTables: NewTables()}

	t.Run("only use job table if no filters", func(t *testing.T) {
		tables, err := qb.determineTablesForQuery([]map[string]bool{})
		assert.NoError(t, err)
		assert.Equal(t, util.StringListToSet([]string{jobTable}), tables)
	})

	t.Run("only use job table if only querying for field in it", func(t *testing.T) {
		tables, err := qb.determineTablesForQuery([]map[string]bool{
			util.StringListToSet([]string{jobTable}),
		})
		assert.NoError(t, err)
		assert.Equal(t, util.StringListToSet([]string{jobTable}), tables)
	})

	t.Run("use highest precedence table if querying for field in multiple tables", func(t *testing.T) {
		tables, err := qb.determineTablesForQuery([]map[string]bool{
			util.StringListToSet([]string{jobTable, jobRunTable, userAnnotationLookupTable}),
		})
		assert.NoError(t, err)
		assert.Equal(t, util.StringListToSet([]string{jobTable}), tables)
	})

	t.Run("only use user_annotation_lookup if querying by queue and annotation", func(t *testing.T) {
		tables, err := qb.determineTablesForQuery([]map[string]bool{
			util.StringListToSet([]string{jobTable, jobRunTable, userAnnotationLookupTable}),
			util.StringListToSet([]string{userAnnotationLookupTable}),
		})
		assert.NoError(t, err)
		assert.Equal(t, util.StringListToSet([]string{userAnnotationLookupTable}), tables)
	})

	t.Run("return multiple tables if there is no overlap", func(t *testing.T) {
		tables, err := qb.determineTablesForQuery([]map[string]bool{
			util.StringListToSet([]string{jobTable, userAnnotationLookupTable}),
			util.StringListToSet([]string{jobRunTable}),
		})
		assert.NoError(t, err)
		assert.Equal(t, util.StringListToSet([]string{jobTable, jobRunTable}), tables)
	})

	t.Run("many fields with no overlap", func(t *testing.T) {
		tables, err := qb.determineTablesForQuery([]map[string]bool{
			util.StringListToSet([]string{jobTable, jobRunTable}),
			util.StringListToSet([]string{jobRunTable}),
			util.StringListToSet([]string{jobRunTable}),
			util.StringListToSet([]string{userAnnotationLookupTable}),
		})
		assert.NoError(t, err)
		assert.Equal(t, util.StringListToSet([]string{jobRunTable, userAnnotationLookupTable}), tables)
	})
}

func TestQueryBuilder_CreateTempTable(t *testing.T) {
	query, tempTableName := NewQueryBuilder(NewTables()).CreateTempTable()
	assert.NotEmpty(t, tempTableName)
	assert.Equal(t, splitByWhitespace(
		fmt.Sprintf("CREATE TEMPORARY TABLE %s ( job_id varchar(32) NOT NULL ) ON COMMIT DROP", tempTableName)),
		splitByWhitespace(query.Sql))
	assert.Equal(t, []interface{}{}, query.Args)
}

func TestQueryBuilder_JobCountEmpty(t *testing.T) {
	query, err := NewQueryBuilder(NewTables()).JobCount([]*model.Filter{})
	assert.NoError(t, err)
	assert.Equal(t, splitByWhitespace("SELECT COUNT(*) FROM job AS j"),
		splitByWhitespace(query.Sql))
	assert.Equal(t, []interface{}(nil), query.Args)
}

func TestQueryBuilder_JobCount(t *testing.T) {
	query, err := NewQueryBuilder(NewTables()).JobCount(testFilters)
	assert.NoError(t, err)
	assert.Equal(t, splitByWhitespace(`
			SELECT COUNT(DISTINCT j.job_id) FROM job AS j
			INNER JOIN (
				SELECT job_id
				FROM user_annotation_lookup
				WHERE (key = $1 AND value = $2) OR (key = $3 AND value LIKE $4)
				GROUP BY job_id
				HAVING COUNT(*) = 2
			) AS aft ON j.job_id = aft.job_id
			WHERE j.queue = $5 AND j.owner LIKE $6
		`),
		splitByWhitespace(query.Sql))
	assert.Equal(t, []interface{}{"1234", "abcd", "5678", "efgh%", "test-queue", "anon%"}, query.Args)
}

func TestQueryBuilder_InsertIntoTempTableEmpty(t *testing.T) {
	query, err := NewQueryBuilder(NewTables()).InsertIntoTempTable(
		"test_table",
		[]*model.Filter{},
		nil,
		0,
		10,
	)
	assert.NoError(t, err)
	assert.Equal(t, splitByWhitespace(`
			INSERT INTO test_table (job_id)
			SELECT j.job_id
			FROM job AS j
			LIMIT 10 OFFSET 0
			ON CONFLICT DO NOTHING
		`),
		splitByWhitespace(query.Sql))
	assert.Equal(t, []interface{}(nil), query.Args)
}

func TestQueryBuilder_InsertIntoTempTable(t *testing.T) {
	query, err := NewQueryBuilder(NewTables()).InsertIntoTempTable(
		"test_table",
		testFilters,
		&model.Order{
			Direction: "ASC",
			Field:     "jobId",
		},
		0,
		10,
	)
	assert.NoError(t, err)
	assert.Equal(t, splitByWhitespace(`
			INSERT INTO test_table (job_id)
			SELECT j.job_id FROM job AS j
			INNER JOIN (
				SELECT job_id
				FROM user_annotation_lookup
				WHERE (key = $1 AND value = $2) OR (key = $3 AND value LIKE $4)
				GROUP BY job_id
				HAVING COUNT(*) = 2
			) AS aft ON j.job_id = aft.job_id
			WHERE j.queue = $5 AND j.owner LIKE $6
			ORDER BY j.job_id ASC
			LIMIT 10 OFFSET 0
			ON CONFLICT DO NOTHING
		`),
		splitByWhitespace(query.Sql))
	assert.Equal(t, []interface{}{"1234", "abcd", "5678", "efgh%", "test-queue", "anon%"}, query.Args)
}

func TestQueryBuilder_CountGroupsEmpty(t *testing.T) {
	query, err := NewQueryBuilder(NewTables()).CountGroups(
		[]*model.Filter{},
		"state",
	)
	assert.NoError(t, err)
	assert.Equal(t, splitByWhitespace(`
			SELECT COUNT(*) FROM (
			    SELECT j.state
			    FROM job AS j
			    GROUP BY j.state
			) AS group_table
		`),
		splitByWhitespace(query.Sql))
	assert.Equal(t, []interface{}(nil), query.Args)
}

func TestQueryBuilder_CountGroups(t *testing.T) {
	query, err := NewQueryBuilder(NewTables()).CountGroups(
		testFilters,
		"state",
	)
	assert.NoError(t, err)
	assert.Equal(t, splitByWhitespace(`
			SELECT COUNT(*) FROM (
			    SELECT j.state
			    FROM job AS j
				INNER JOIN (
					SELECT job_id
					FROM user_annotation_lookup
					WHERE (key = $1 AND value = $2) OR (key = $3 AND value LIKE $4)
					GROUP BY job_id
					HAVING COUNT(*) = 2
				) AS aft ON j.job_id = aft.job_id
			    WHERE j.queue = $5 AND j.owner LIKE $6
			    GROUP BY j.state
			) AS group_table
		`),
		splitByWhitespace(query.Sql))
	assert.Equal(t, []interface{}{"1234", "abcd", "5678", "efgh%", "test-queue", "anon%"}, query.Args)
}

func TestQueryBuilder_GroupByEmpty(t *testing.T) {
	query, err := NewQueryBuilder(NewTables()).GroupBy(
		[]*model.Filter{},
		nil,
		"jobSet",
		[]string{},
		0,
		10,
	)
	assert.NoError(t, err)
	assert.Equal(t, splitByWhitespace(`
			SELECT j.jobset, COUNT(*) AS count
			FROM job AS j
			GROUP BY j.jobset
			LIMIT 10 OFFSET 0
		`),
		splitByWhitespace(query.Sql))
	assert.Equal(t, []interface{}(nil), query.Args)
}

func TestQueryBuilder_GroupBy(t *testing.T) {
	query, err := NewQueryBuilder(NewTables()).GroupBy(
		testFilters,
		&model.Order{
			Direction: "DESC",
			Field:     "count",
		},
		"jobSet",
		[]string{},
		0,
		10,
	)
	assert.NoError(t, err)
	assert.Equal(t, splitByWhitespace(`
			SELECT j.jobset, COUNT(*) AS count
			FROM job AS j
			INNER JOIN (
				SELECT job_id
				FROM user_annotation_lookup
				WHERE (key = $1 AND value = $2) OR (key = $3 AND value LIKE $4)
				GROUP BY job_id
				HAVING COUNT(*) = 2
			) AS aft ON j.job_id = aft.job_id
			WHERE j.queue = $5 AND j.owner LIKE $6
			GROUP BY j.jobset
			ORDER BY count DESC
			LIMIT 10 OFFSET 0
		`),
		splitByWhitespace(query.Sql))
	assert.Equal(t, []interface{}{"1234", "abcd", "5678", "efgh%", "test-queue", "anon%"}, query.Args)
}

func TestQueryBuilder_GroupBySingleAggregate(t *testing.T) {
	query, err := NewQueryBuilder(NewTables()).GroupBy(
		testFilters,
		&model.Order{
			Direction: "ASC",
			Field:     "submitted",
		},
		"jobSet",
		[]string{
			"submitted",
		},
		20,
		100,
	)
	assert.NoError(t, err)
	assert.Equal(t, splitByWhitespace(`
			SELECT j.jobset, COUNT(*) AS count, MAX(j.submitted) AS submitted
			FROM job AS j
			INNER JOIN (
				SELECT job_id
				FROM user_annotation_lookup
				WHERE (key = $1 AND value = $2) OR (key = $3 AND value LIKE $4)
				GROUP BY job_id
				HAVING COUNT(*) = 2
			) AS aft ON j.job_id = aft.job_id
			WHERE j.queue = $5 AND j.owner LIKE $6
			GROUP BY j.jobset
			ORDER BY submitted ASC
			LIMIT 100 OFFSET 20
		`),
		splitByWhitespace(query.Sql))
	assert.Equal(t, []interface{}{"1234", "abcd", "5678", "efgh%", "test-queue", "anon%"}, query.Args)
}

func TestQueryBuilder_GroupByMultipleAggregates(t *testing.T) {
	query, err := NewQueryBuilder(NewTables()).GroupBy(
		testFilters,
		&model.Order{
			Direction: "DESC",
			Field:     "lastTransitionTime",
		},
		"jobSet",
		[]string{
			"lastTransitionTime",
			"submitted",
		},
		20,
		100,
	)
	assert.NoError(t, err)
	assert.Equal(t, splitByWhitespace(`
			SELECT j.jobset, COUNT(*) AS count, AVG(j.last_transition_time_seconds) AS last_transition_time_seconds, MAX(j.submitted) AS submitted
			FROM job AS j
			INNER JOIN (
				SELECT job_id
				FROM user_annotation_lookup
				WHERE (key = $1 AND value = $2) OR (key = $3 AND value LIKE $4)
				GROUP BY job_id
				HAVING COUNT(*) = 2
			) AS aft ON j.job_id = aft.job_id
			WHERE j.queue = $5 AND j.owner LIKE $6
			GROUP BY j.jobset
			ORDER BY last_transition_time_seconds DESC
			LIMIT 100 OFFSET 20
		`),
		splitByWhitespace(query.Sql))
	assert.Equal(t, []interface{}{"1234", "abcd", "5678", "efgh%", "test-queue", "anon%"}, query.Args)
}

func splitByWhitespace(s string) []string {
	return strings.FieldsFunc(s, splitFn)
}

func splitFn(r rune) bool {
	return r == ' ' || r == '\n' || r == '\t'
}
