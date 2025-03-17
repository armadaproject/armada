package util

import (
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/util/yaml"
)

func BindJsonOrYaml(filePath string, obj interface{}) error {
	reader, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed opening file %s due to %s", filePath, err)
	}
	err = yaml.NewYAMLOrJSONDecoder(reader, 128).Decode(obj)
	if err != nil {
		return fmt.Errorf("failed to parse file %s because: %v", filePath, err)
	}
	return nil
}
