package cmd

import (
	"context"
	"k8s.io/apimachinery/pkg/util/yaml"
	"log"
	"os"
	"time"
)

func bindYaml(filePath string, obj interface{}) {
	yamlReader, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.NewYAMLOrJSONDecoder(yamlReader, 128).Decode(obj)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
}

func timeout() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	return ctx
}
