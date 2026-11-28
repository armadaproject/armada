package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/lookout/gen/models"
)

func TestList_SendsTheRequest(t *testing.T) {
	f, url := newFakeLookout(t)
	args := ListArgs{
		Filters:       []*models.Filter{{Field: "queue", Match: "anyOf", Value: []string{"a", "b"}}, {Field: "team", Match: "exact", Value: "data", IsAnnotation: true}},
		Order:         models.Order{Field: "queue", Direction: "ASC"},
		ActiveJobSets: true,
		Skip:          5,
		Take:          20,
	}

	_, err := List(connectionTo(url))(args)

	require.NoError(t, err)
	r := f.requests[0]
	assert.Equal(t, []any{
		map[string]any{"field": "queue", "match": "anyOf", "value": []any{"a", "b"}},
		map[string]any{"field": "team", "match": "exact", "value": "data", "isAnnotation": true},
	}, r["filters"])
	assert.Equal(t, map[string]any{"field": "queue", "direction": "ASC"}, r["order"])
	assert.Equal(t, true, r["activeJobSets"])
	assert.EqualValues(t, 5, r["skip"])
	assert.EqualValues(t, 20, r["take"])
}

// Lookout treats null filters as matching nothing, so no filters must be sent as an empty list.
func TestList_SendsNoFiltersAsAnEmptyList(t *testing.T) {
	f, url := newFakeLookout(t)

	_, err := List(connectionTo(url))(ListArgs{Order: models.Order{Field: "submitted", Direction: "DESC"}, Take: 1})

	require.NoError(t, err)
	assert.Equal(t, []any{}, f.requests[0]["filters"])
}

func TestList_ReturnsAnEmptyListWhenNothingMatches(t *testing.T) {
	_, url := newFakeLookout(t)

	jobs, err := List(connectionTo(url))(ListArgs{Order: models.Order{Field: "submitted", Direction: "DESC"}, Take: 1})

	require.NoError(t, err)
	assert.NotNil(t, jobs)
	assert.Empty(t, jobs)
}

// Lookout ignores the value of an exists filter, but its request check rejects a filter that has none.
func TestList_SendsAPlaceholderValueForExists(t *testing.T) {
	f, url := newFakeLookout(t)

	_, err := List(connectionTo(url))(ListArgs{
		Filters: []*models.Filter{{Field: "team", Match: "exists", IsAnnotation: true}},
		Order:   models.Order{Field: "submitted", Direction: "DESC"},
		Take:    1,
	})

	require.NoError(t, err)
	assert.Equal(t, []any{map[string]any{"field": "team", "match": "exists", "value": "", "isAnnotation": true}}, f.requests[0]["filters"])
}

// A mistake in the request is reported before anything is sent, and so before any login.
func TestList_RejectsAnInvalidRequestBeforeSendingAnything(t *testing.T) {
	f, url := newFakeLookout(t)

	_, err := List(connectionTo(url))(ListArgs{
		Filters: []*models.Filter{{Field: "queue", Match: "exact"}},
		Order:   models.Order{Field: "submitted", Direction: "DESC"},
		Take:    1,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "filter 1: value in body is required")
	assert.Empty(t, f.requests)
}

func TestListArgsValidate(t *testing.T) {
	order := models.Order{Field: "submitted", Direction: "DESC"}
	tests := map[string]struct {
		args    ListArgs
		wantErr string // empty when the args are valid
	}{
		"no filters":                       {args: ListArgs{Order: order}},
		"a filter with a value":            {args: ListArgs{Order: order, Filters: []*models.Filter{{Field: "queue", Match: "exact", Value: "q"}}}},
		"an exists filter without a value": {args: ListArgs{Order: order, Filters: []*models.Filter{{Field: "team", Match: "exists", IsAnnotation: true}}}},
		"a filter without a value":         {args: ListArgs{Order: order, Filters: []*models.Filter{{Field: "queue", Match: "exact"}}}, wantErr: "filter 1: value in body is required"},
		"a filter without a field":         {args: ListArgs{Order: order, Filters: []*models.Filter{{Match: "exact", Value: "q"}}}, wantErr: "filter 1: field in body is required"},
		"a filter without a match":         {args: ListArgs{Order: order, Filters: []*models.Filter{{Field: "queue", Value: "q"}}}, wantErr: "filter 1: match in body is required"},
		"a filter with an unknown match":   {args: ListArgs{Order: order, Filters: []*models.Filter{{Field: "queue", Match: "exacct", Value: "q"}}}, wantErr: "match in body should be one of"},
		"a filter lacking everything":      {args: ListArgs{Order: order, Filters: []*models.Filter{{}}}, wantErr: "field in body is required; match in body is required"},
		"the second filter being invalid": {
			args:    ListArgs{Order: order, Filters: []*models.Filter{{Field: "a", Match: "exact", Value: "x"}, {Field: "b", Match: "exact"}}},
			wantErr: "filter 2: value in body is required",
		},
		"a null filter":                {args: ListArgs{Order: order, Filters: []*models.Filter{nil}}, wantErr: "filter 1 is null"},
		"an order without a direction": {args: ListArgs{Order: models.Order{Field: "queue"}}, wantErr: "direction in body is required"},
		"an order without a field":     {args: ListArgs{Order: models.Order{Direction: "ASC"}}, wantErr: "field in body is required"},
		"an order with an unknown direction": {
			args: ListArgs{Order: models.Order{Field: "queue", Direction: "UP"}}, wantErr: "direction in body should be one of",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := tc.args.Validate()

			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.NotContains(t, err.Error(), "\n", "the message is one line")
		})
	}
}

func TestValidateFilters_DoesNotChangeTheFilters(t *testing.T) {
	filters := []*models.Filter{{Field: "team", Match: "exists", IsAnnotation: true}}

	require.NoError(t, ValidateFilters(filters))

	assert.Nil(t, filters[0].Value, "the placeholder is only added to the copy that is sent")
}
