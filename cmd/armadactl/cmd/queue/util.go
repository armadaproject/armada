package queue

import (
	"fmt"
	"strconv"
)

type FlagGetStringToString func(string) (map[string]string, error)

func (f FlagGetStringToString) ToFloat64(flagName string) (map[string]float64, error) {
	limits, err := f(flagName)
	if err != nil {
		return nil, err
	}

	result := make(map[string]float64, len(limits))
	for resourceName, limit := range limits {
		limitFloat, err := strconv.ParseFloat(limit, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s as float64. %s", resourceName, err)
		}
		result[resourceName] = limitFloat
	}

	return result, nil
}
