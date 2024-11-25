package testfixtures

import (
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

func TestSimpleNode(id string) *internaltypes.Node {
	return internaltypes.CreateNode(
		id,
		nil,
		0,
		"",
		"",
		"",
		nil,
		nil,
		internaltypes.ResourceList{},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil)
}
