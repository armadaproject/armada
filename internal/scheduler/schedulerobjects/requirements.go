package schedulerobjects

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
)

func (info *JobSchedulingInfo) GetTotalResourceRequest() ResourceList {
	rv := ResourceList{}
	for _, oreq := range info.ObjectRequirements {
		if preq := oreq.GetPodRequirements(); preq != nil {
			rv.Add(ResourceListFromV1ResourceList(preq.ResourceRequirements.Requests))
		}
	}
	return rv
}

func CalculateHashFromPodRequirements(reqs *PodRequirements) ([]byte, error) {
	if reqs == nil {
		return []byte{}, errors.Errorf("cannot calculate hash of nil pod requirements")
	}
	podReqsCopy := proto.Clone(reqs).(*PodRequirements)
	// Clear annotations, as otherwise it'll cause the hash to be different for equivalent requirements
	// Ideally long term we'd remove high cardinality annotations from the PodRequirements
	podReqsCopy.Annotations = map[string]string{}
	reqsHash, err := protoutil.Hash(podReqsCopy)
	if err != nil {
		return []byte{}, err
	}
	return reqsHash, nil
}
