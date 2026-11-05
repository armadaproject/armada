package job

import (
	"fmt"
	"strings"

	openapierrors "github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/lookout/gen/models"
)

// Validate checks the filters and the order against the rules Lookout checks a request against, so a mistake is
// reported before anything is sent, and before any login, which may be interactive.
func (a ListArgs) Validate() error {
	if err := ValidateFilters(a.Filters); err != nil {
		return err
	}
	return ValidateOrder(a.Order)
}

// ValidateFilters checks filters as Lookout does. The exists match only looks at the field, so it may leave the
// value out, as it is when the request is sent.
func ValidateFilters(filters []*models.Filter) error {
	for i, f := range requestFilters(filters) {
		if f == nil {
			return fmt.Errorf("filter %d is null", i+1)
		}
		if err := f.Validate(strfmt.Default); err != nil {
			return fmt.Errorf("filter %d: %s", i+1, validationMessage(err))
		}
	}
	return nil
}

// ValidateOrder checks an order as Lookout does.
func ValidateOrder(order models.Order) error {
	if err := order.Validate(strfmt.Default); err != nil {
		return errors.New(validationMessage(err))
	}
	return nil
}

// validationMessage is the message of a validation error on one line, with the reasons it lists.
func validationMessage(err error) string {
	var composite *openapierrors.CompositeError
	if !errors.As(err, &composite) || len(composite.Errors) == 0 {
		return err.Error()
	}
	reasons := make([]string, len(composite.Errors))
	for i, e := range composite.Errors {
		reasons[i] = e.Error()
	}
	return strings.Join(reasons, "; ")
}

// requestFilters returns the filters as they are sent: a copy, and never nil, because Lookout treats null filters
// as matching nothing. Lookout's request check wants every filter to have a value, though it ignores the value
// of an exists filter, which only looks at the field, so an empty one is given to an exists filter without one.
func requestFilters(filters []*models.Filter) []*models.Filter {
	out := make([]*models.Filter, len(filters))
	for i, f := range filters {
		if f != nil && f.Match == models.FilterMatchExists && f.Value == nil {
			withValue := *f
			withValue.Value = ""
			f = &withValue
		}
		out[i] = f
	}
	return out
}
