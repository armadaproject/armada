package testsuite

import (
	"fmt"
	"testing"

	"github.com/G-Research/armada/pkg/api"
	"github.com/stretchr/testify/assert"
)

func TestFoo(t *testing.T) {
	msg := &api.EventMessage{
		Events: &api.EventMessage_Cancelled{
			Cancelled: &api.JobCancelledEvent{},
		},
	}

	fmt.Println(stringFromApiEvent(msg))
	assert.Fail(t, "foo")
}
