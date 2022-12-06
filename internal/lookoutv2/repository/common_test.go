package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/common/util"
)

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
