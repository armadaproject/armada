package queue

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
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

func validateQueueName(cmd *cobra.Command, args []string) error {
	switch n := len(args); {
	case n == 0:
		return fmt.Errorf("must provide <queue_name>")
	case n != 1:
		return fmt.Errorf("accepts only one argument for <queue_name>")
	default:
		return nil
	}
}
