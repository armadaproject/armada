package startup

import (
	"regexp"
	"strconv"
	"time"

	"github.com/gogo/protobuf/types"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

var (
	pulledPattern = regexp.MustCompile(
		`^Successfully pulled image "[^"]*" in (\S+) \((\S+) including waiting\)\. Image size: (\d+) bytes\.$`)
	alreadyPresentPattern = regexp.MustCompile(
		`^Container image "[^"]*" already present on machine`)
)

// parsePulledMessage reads a kubelet Pulled event message for image pull information
// returning nil when no matching event can be found.
func parsePulledMessage(message string) *armadaevents.ImagePullInfo {
	if alreadyPresentPattern.MatchString(message) {
		return &armadaevents.ImagePullInfo{Cached: true}
	}

	match := pulledPattern.FindStringSubmatch(message)
	if match == nil {
		return nil
	}

	pullDuration, err := time.ParseDuration(match[1])
	if err != nil {
		return nil
	}
	includingWaiting, err := time.ParseDuration(match[2])
	if err != nil {
		return nil
	}
	sizeBytes, err := strconv.ParseUint(match[3], 10, 64)
	if err != nil {
		return nil
	}

	waiting := includingWaiting - pullDuration
	if waiting < 0 {
		waiting = 0
	}

	pull := &armadaevents.ImagePullInfo{
		Duration:    types.DurationProto(pullDuration),
		WaitingTime: types.DurationProto(waiting),
	}
	// Kubelet uses 0 to indicate unknown, so we only set if non-zero
	if sizeBytes > 0 {
		pull.SizeBytes = &types.UInt64Value{Value: sizeBytes}
	}
	return pull
}
