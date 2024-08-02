package cmd

import "fmt"

func queueNameValidation(queueName string) error {
	if queueName == "" {
		return fmt.Errorf("cannot provide empty queue name")
	}
	return nil
}
