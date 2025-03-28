package pointer

import (
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Now returns a pointer to the current time
func Now() *time.Time {
	return Time(time.Now())
}

// Time returns a pointer to supplied time
func Time(t time.Time) *time.Time {
	return &t
}

func MustParseResource(s string) *resource.Quantity {
	qty := resource.MustParse(s)
	return &qty
}
