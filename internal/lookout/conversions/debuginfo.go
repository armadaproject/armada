package conversions

import (
	"encoding/json"

	"github.com/armadaproject/armada/internal/lookout/gen/models"
)

const supportedDebugSchemaVersion = 1

func ParseDebugInfo(message string) *models.PodDebugInfo {
	info := &models.PodDebugInfo{}
	if err := json.Unmarshal([]byte(message), info); err != nil {
		return nil
	}
	if info.SchemaVersion != supportedDebugSchemaVersion || info.Trigger == "" || info.Pod == nil {
		return nil
	}

	info.Pod.InitContainers = dropNil(info.Pod.InitContainers)
	info.Pod.Containers = dropNil(info.Pod.Containers)
	info.Pod.Events = dropNil(info.Pod.Events)
	if info.Node != nil {
		info.Node.Conditions = dropNil(info.Node.Conditions)
		info.Node.Events = dropNil(info.Node.Events)
	}
	return info
}

func dropNil[T any](entries []*T) []*T {
	kept := entries[:0]
	for _, entry := range entries {
		if entry != nil {
			kept = append(kept, entry)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}
