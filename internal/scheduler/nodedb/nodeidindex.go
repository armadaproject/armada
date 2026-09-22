package nodedb

import (
	"fmt"
	"reflect"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

// NodeIndex is an index for internaltypes.Node that indexes Node.GetId()
type nodeIdIndex struct{}

func createNodeIdIndex() *nodeIdIndex {
	return &nodeIdIndex{}
}

func (nii *nodeIdIndex) FromObject(obj interface{}) (bool, []byte, error) {
	node, ok := obj.(*internaltypes.Node)
	if !ok {
		return false, nil,
			fmt.Errorf("Expected type *Node but got %v", reflect.TypeOf(obj))
	}
	val := node.GetId()
	if val == "" {
		return false, nil, nil
	}

	// Add the null character as a terminator.
	// Single allocation: the previous val += "\x00" + []byte(val) did two.
	key := make([]byte, 0, len(val)+1)
	key = append(key, val...)
	key = append(key, 0)
	return true, key, nil
}

func (nii *nodeIdIndex) FromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("must provide only a single argument")
	}
	arg, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("argument must be a string: %#v", args[0])
	}
	// Add the null character as a terminator.
	// Single allocation: the previous arg += "\x00" + []byte(arg) did two.
	key := make([]byte, 0, len(arg)+1)
	key = append(key, arg...)
	key = append(key, 0)
	return key, nil
}

func (nii *nodeIdIndex) PrefixFromArgs(args ...interface{}) ([]byte, error) {
	panic("PrefixFromArgs not implemented")
}
