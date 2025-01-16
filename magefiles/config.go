package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type BuildConfig struct {
	DockerRegistries       map[string]string `json:"dockerRegistries"`
	PythonBuilderBaseImage string            `json:"pythonBuilderBaseImage"`
	ScalaBuilderBaseImage  string            `json:"scalaBuilderBaseImage"`
}

func getBuildConfig() (BuildConfig, error) {
	configPath := os.Getenv("ARMADA_BUILD_CONFIG")
	if configPath == "" {
		return BuildConfig{}, nil
	}

	content, err := os.ReadFile(configPath)
	if err != nil {
		return BuildConfig{}, fmt.Errorf("error reading file: %w", err)
	}

	var config BuildConfig
	err = json.Unmarshal(content, &config)
	if err != nil {
		return BuildConfig{}, fmt.Errorf("error parsing JSON: %w", err)
	}

	return config, nil
}
