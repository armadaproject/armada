package testutil

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/testing/protocmp"
)

func AssertProtoEqual(t *testing.T, expected, actual any, msgAndArgs ...any) bool {
	diff := cmp.Diff(expected, actual, protocmp.Transform())
	if diff != "" {
		assert.Fail(t, fmt.Sprintf("Not equal: \n"+
			"expected: %s\n"+
			"actual  : %s%s", expected, actual, diff), msgAndArgs...)
		return false
	}
	return true
}

func RequireProtoEqual(t *testing.T, expected, actual any, msgAndArgs ...any) {
	if AssertProtoEqual(t, expected, actual, msgAndArgs...) {
		return
	}
	t.FailNow()
}
