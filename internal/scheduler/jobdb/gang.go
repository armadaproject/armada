package jobdb

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
)

type GangInfo struct {
	id             string
	cardinality    int
	nodeUniformity string
}

var basicJobGangInfo = GangInfo{
	id:             "",
	cardinality:    1,
	nodeUniformity: "",
}

// BasicJobGangInfo The info used for non-gang jobs
func BasicJobGangInfo() GangInfo {
	return basicJobGangInfo
}

func CreateGangInfo(id string, cardinality int, nodeUniformity string) GangInfo {
	return GangInfo{
		id:             id,
		cardinality:    cardinality,
		nodeUniformity: nodeUniformity,
	}
}

func (g GangInfo) IsGang() bool {
	return g.cardinality > 1
}

func (g GangInfo) Id() string {
	return g.id
}

func (g GangInfo) Cardinality() int {
	return g.cardinality
}

func (g GangInfo) NodeUniformity() string {
	return g.nodeUniformity
}

func (g GangInfo) Equal(other GangInfo) bool {
	// Currently we only have comparable fields, so we can rely on simple equality check
	return g == other
}

func (g GangInfo) String() string {
	return fmt.Sprintf("id: %s cardinality: %d uniformity label: %s gang: %t", g.Id(), g.Cardinality(), g.NodeUniformity(), g.IsGang())
}

func GangInfoFromMinimalJob(job interfaces.MinimalJob) (*GangInfo, error) {
	basicGangInfo := BasicJobGangInfo()

	// Prefer structured Gang field if available (new style or normalized from annotations)
	gangProto := job.Gang()
	if gangProto != nil {
		gangId := gangProto.GangId
		if gangId == "" {
			return nil, errors.Errorf("gang id is empty")
		}

		gangCardinality := int(gangProto.Cardinality)
		if gangCardinality <= 0 {
			return nil, errors.Errorf("gang cardinality %d is non-positive", gangCardinality)
		}
		if gangCardinality < 2 {
			return &basicGangInfo, nil
		}

		gangInfo := CreateGangInfo(gangId, gangCardinality, gangProto.NodeUniformityLabelName)
		return &gangInfo, nil
	}

	// Parse gang metadata from PodRequirements annotations for backward compatibility.
	// Jobs already stored in the database have gang metadata embedded in annotations within
	// the scheduling_info protobuf blob, not in a structured Gang protobuf field.
	// This fallback is required to support existing jobs until a database migration is performed.
	annotations := job.Annotations()
	gangId, ok := annotations[constants.GangIdAnnotation]
	if !ok {
		return &basicGangInfo, nil
	}
	if gangId == "" {
		return nil, errors.Errorf("gang id is empty")
	}

	gangCardinalityString, ok := annotations[constants.GangCardinalityAnnotation]
	if !ok {
		return nil, errors.Errorf("gang cardinality annotation %s is missing", constants.GangCardinalityAnnotation)
	}
	gangCardinality, err := strconv.Atoi(gangCardinalityString)
	if err != nil {
		return nil, fmt.Errorf("gang cardinality is not parseable - %s", errors.WithStack(err))
	}
	if gangCardinality <= 0 {
		return nil, errors.Errorf("gang cardinality %d is non-positive", gangCardinality)
	}
	if gangCardinality < 2 {
		return &basicGangInfo, nil
	}

	nodeUniformityLabel := job.Annotations()[constants.GangNodeUniformityLabelAnnotation]
	gangInfo := CreateGangInfo(gangId, gangCardinality, nodeUniformityLabel)
	return &gangInfo, nil
}
