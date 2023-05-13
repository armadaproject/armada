package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/magefile/mage/mg"
)

var dlvString string = "dlv debug --listen=:4000 --headless=true --log=true --accept-multiclient --api-version=2 --continue --output __debug_server cmd/%s/main.go -- "
var migrateString string = "go run ./cmd/%s/main.go"
var imageName string = "go-delve"

func createDelveImage() error {
	dockerFile := "./developer/debug/Dockerfile"

	err := dockerRun("build", "-t", imageName, "-f", dockerFile, ".")
	if err != nil {
		return err
	}

	return nil
}

func CreateDelveCompose() error {
	mg.Deps(createDelveImage)

	data, err := ioutil.ReadFile("docker-compose.yaml")
	if err != nil {
		return err
	}

	var binaryName string

	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		// If line is 2 spaces out, we know it's a service
		if strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "    ") {
			binaryName = strings.Split(strings.TrimPrefix(line, "  "), ":")[0]
			if binaryName == "server" {
				binaryName = "armada"
			}
		}
		if strings.Contains(line, "image: ${ARMADA_IMAGE") {
			lines[i] = "    image: " + imageName
		}
		if strings.HasPrefix(line, "    command: ./") {
			lines[i] = strings.Replace(line, "./"+binaryName, fmt.Sprintf(dlvString, binaryName), 1)
		}
		if strings.HasPrefix(line, "    entrypoint: ") && strings.Contains(line, "./lookout") {
			// This time, replace the first instance of ./lookout with the migrate command
			lines[i] = strings.Replace(line, "./"+binaryName, fmt.Sprintf(migrateString, binaryName), 1)
			// Then, replace the second instance of ./lookout with the dlv command
			lines[i] = strings.Replace(lines[i], "./"+binaryName, fmt.Sprintf(dlvString, binaryName), 1)
		}
	}

	output := strings.Join(lines, "\n")
	err = ioutil.WriteFile("docker-compose.dev.yaml", []byte(output), os.ModePerm)
	if err != nil {
		return err
	}

	fmt.Println("Create Delve Compose File successfully!")
	return nil
}

func Debug() error {
	mg.Deps(CreateDelveCompose, mg.F(LocalDev, "debug"))

	return dockerRun("compose", "-f", "docker-compose.dev.yaml", "up", "--build")
}
