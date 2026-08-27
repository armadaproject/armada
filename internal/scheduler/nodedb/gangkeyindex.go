package nodedb

import (
	"fmt"
	"reflect"
)

// gangKeyIndex indexes *EvictedJobSchedulingContext by the gang key of the
// evicted job, i.e. the tuple (queue, gang id). It is a non-unique index: all
// evicted members of the same gang share a single key, so a lookup returns
// every evicted sibling.
//
// Non-gang jobs are not indexed (FromObject returns ok=false), so the index
// only ever contains gang members.
type gangKeyIndex struct{}

func createGangKeyIndex() *gangKeyIndex {
	return &gangKeyIndex{}
}

// gangKeyBytes encodes a (queue, gangId) pair into the index key. A null
// terminator separates the two fields so that distinct pairs cannot collide
// (e.g. ("a", "bc") vs ("ab", "c")).
func gangKeyBytes(queue, gangId string) []byte {
	return []byte(queue + "\x00" + gangId + "\x00")
}

func (gki *gangKeyIndex) FromObject(obj interface{}) (bool, []byte, error) {
	esc, ok := obj.(*EvictedJobSchedulingContext)
	if !ok {
		return false, nil, fmt.Errorf("expected type *EvictedJobSchedulingContext but got %v", reflect.TypeOf(obj))
	}
	job := esc.JobSchedulingContext.Job
	if job == nil || !job.IsInGang() {
		// Only gang members are indexed.
		return false, nil, nil
	}
	return true, gangKeyBytes(job.Queue(), job.GetGangInfo().Id()), nil
}

func (gki *gangKeyIndex) FromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("must provide exactly two arguments (queue, gangId)")
	}
	queue, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("first argument (queue) must be a string: %#v", args[0])
	}
	gangId, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("second argument (gangId) must be a string: %#v", args[1])
	}
	return gangKeyBytes(queue, gangId), nil
}
