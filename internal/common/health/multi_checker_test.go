package health

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultiChecker_WhenNoChecksAdded_CheckFails(t *testing.T) {
	mc := NewMultiChecker()
	err := mc.Check()
	assert.Error(t, err)
}

func TestMultiChecker_TwoChecksPass_CheckPasses(t *testing.T) {
	mc := NewMultiChecker(&mockChecker{}, &mockChecker{})

	err := mc.Check()
	assert.Nil(t, err)
}

func TestMultiChecker_TwoChecksAndFirstFails_CheckFails(t *testing.T) {
	err := errors.New("Error1")
	mc := NewMultiChecker(&mockChecker{err: err}, &mockChecker{})

	assert.Equal(t, err, mc.Check())
}

func TestMultiChecker_TwoChecksSecondFails_CheckFails(t *testing.T) {
	err := errors.New("Error2")
	mc := NewMultiChecker(&mockChecker{}, &mockChecker{err: err})

	assert.Equal(t, err, mc.Check())
}

func TestMultiChecker_TwoChecksBothFail_CheckFails(t *testing.T) {
	err1 := errors.New("Error1")
	err2 := errors.New("Error2")

	mc := NewMultiChecker(&mockChecker{err: err1}, &mockChecker{err: err2})

	expectedError := errors.New(err1.Error() + "\n" + err2.Error())
	assert.Equal(t, expectedError, mc.Check())
}

func TestMultiChecker_TwoChecks_OneViaConstructor_OneViaAdd_BothFail_CheckFails(t *testing.T) {
	err1 := errors.New("Error1")
	err2 := errors.New("Error2")

	mc := NewMultiChecker(&mockChecker{err: err1})
	mc.Add(&mockChecker{err: err2})

	expectedError := errors.New(err1.Error() + "\n" + err2.Error())
	assert.Equal(t, expectedError, mc.Check())
}

func TestMultiChecker_TwoChecks_BothViaAdd_BothFail_CheckFails(t *testing.T) {
	err1 := errors.New("Error1")
	err2 := errors.New("Error2")

	mc := NewMultiChecker()
	mc.Add(&mockChecker{err: err1})
	mc.Add(&mockChecker{err: err2})

	expectedError := errors.New(err1.Error() + "\n" + err2.Error())
	assert.Equal(t, expectedError, mc.Check())
}

type mockChecker struct {
	err error
}

func (fmc *mockChecker) Check() error {
	return fmc.err
}
