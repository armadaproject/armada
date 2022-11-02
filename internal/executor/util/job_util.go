package util

import (
	"regexp"

	"github.com/pkg/errors"
)

var (
	armadaJobRegex                 = regexp.MustCompile(`armada-([a-z0-9]+)-0`)
	preemptedMessageRegex          = regexp.MustCompile(`Preempted by ([-a-zA-Z0-9]+)/([-a-zA-Z0-9]+) on node ([-a-zA-Z0-9]+)`)
	invalidJobIdFormat             = "invalid name format: expected 'armada-<UID>-0(-<suffix>)', received '%s'"
	invalidPreemptionMessageFormat = "invalid preemption message: " +
		"expected 'Preempted by ([-a-zA-Z0-9]+)/([-a-zA-Z0-9]+) on node ([-a-zA-Z0-9]+)', received '%s'"
)

type PreemptiveJobInfo struct {
	Namespace string
	Name      string
	Node      string
}

// ParsePreemptionMessage parses the message field from a Preempted Cluster Event
// Message format is 'Preempted by <pod_namespace>/<pod_name> on node <node>'
func ParsePreemptionMessage(msg string) (*PreemptiveJobInfo, error) {
	res := preemptedMessageRegex.FindAllStringSubmatch(msg, -1)
	if len(res) != 1 || len(res[0]) != 4 {
		return nil, errors.Errorf(
			invalidPreemptionMessageFormat,
			msg,
		)
	}

	info := &PreemptiveJobInfo{
		Namespace: res[0][1],
		Name:      res[0][2],
		Node:      res[0][3],
	}

	return info, nil
}

// ExtractJobIdFromName extracts job id from the Armada Job pod
// Pods are named using the convention armada-<UID>-0(-<suffix>)
func ExtractJobIdFromName(name string) (string, error) {
	res := armadaJobRegex.FindAllStringSubmatch(name, -1)
	if len(res) != 1 || len(res[0]) != 2 {
		return "", errors.Errorf(invalidJobIdFormat, name)
	}

	return res[0][1], nil
}
