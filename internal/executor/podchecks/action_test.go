package podchecks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_maxAction(t *testing.T) {
	assert.Equal(t, ActionFail, maxAction(ActionRetry, ActionFail))
	assert.Equal(t, ActionFail, maxAction(ActionFail, ActionRetry))
	assert.Equal(t, ActionRetry, maxAction(ActionWait, ActionRetry))
	assert.Equal(t, ActionWait, maxAction(ActionWait, ActionWait))
}
