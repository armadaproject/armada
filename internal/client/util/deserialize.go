package util

import (
	"os"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func BindJsonOrYaml(filePath string, obj interface{}) {
	reader, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed opening file %s due to %s", filePath, err)
	}
	err = yaml.NewYAMLOrJSONDecoder(reader, 128).Decode(obj)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
}
