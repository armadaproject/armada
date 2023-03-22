package pointer

import "time"

func Pointer[T any](v T) *T {
	return &v
}

// Now returns a pointer to the current time
func Now() *time.Time {
	return Time(time.Now())
}

// Time returns a pointer to supplied time
func Time(t time.Time) *time.Time {
	return &t
}
