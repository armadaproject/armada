package util

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

func UuidsToStrings(uuids []*armadaevents.Uuid) []string {
	result := make([]string, 0, len(uuids))
	for _, uuid := range uuids {
		result = append(result, uuid.String())
	}
	return result
}

func StringUuidsToUuids(uuidStrings []string) ([]armadaevents.Uuid, error) {
	result := make([]armadaevents.Uuid, 0, len(uuidStrings))
	for _, uuidString := range uuidStrings {
		uuid, err := armadaevents.ProtoUuidFromUuidString(uuidString)
		if err != nil {
			return nil, fmt.Errorf("failed to convert uuid string %s to uuid because %s", uuidString, err)
		}
		result = append(result, *uuid)
	}
	return result, nil
}
