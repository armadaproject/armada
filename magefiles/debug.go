package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/magefile/mage/mg"
)

var (
	dlvString     string = "dlv debug --listen=:4000 --headless=true --log=true --accept-multiclient --api-version=2 --continue --output __debug_server cmd/%s/main.go --"
	migrateString string = "go run ./cmd/%s/main.go"
	imageName     string = "go-delve"
)

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

	var folderName string
	var binaryName string
	var newService bool = false

	lines := strings.Split(string(data), "\n")
	for i, line := range lines {

		// Ensures that the binary name is correct
		if strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "    ") {
			folderName = strings.Split(strings.TrimPrefix(line, "  "), ":")[0]
			binaryName = folderName
			// Deal with the edge case that the server binary
			// Has a different config name
			if folderName == "server" {
				folderName = "armada"
			}
			newService = true
		}

		// Correctly inserts the delve command into the docker-compose file
		if strings.HasPrefix(line, "    command: ./") {
			lines[i] = strings.Replace(line, "./"+binaryName, fmt.Sprintf(dlvString, folderName), 1)
			// Add quotes around the command
			lines[i] = strings.Replace(lines[i], "command: ", "command: \"'", 1)
			lines[i] = lines[i] + "'\""
			// Add Delve output file.
			lines[i] = strings.Replace(lines[i], "__debug_server", fmt.Sprintf("./delve/__debug_%s", folderName), 1)
		}

		// Inserts the delve command for the lookout services
		// They have an extra migration step that doesn't need delve.
		if strings.HasPrefix(line, "    entrypoint: ") && strings.Contains(line, "./lookout") {
			// This time, replace the first instance of ./lookout with the migrate command
			lines[i] = strings.Replace(line, "./"+binaryName, fmt.Sprintf(migrateString, folderName), 1)
			// Then, replace the second instance of ./lookout with the dlv command
			lines[i] = strings.Replace(lines[i], "./"+binaryName, fmt.Sprintf(dlvString, folderName), 1)
			// Add Delve output file.
			lines[i] = strings.Replace(lines[i], "__debug_server", fmt.Sprintf("./delve/__debug_%s", folderName), 1)
		}

		// Add a new volume "./:/app" to the service
		if newService && strings.HasPrefix(line, "    volumes:") {
			lines[i] = lines[i] + "\n      - ./:/app:rw"
			newService = false
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

func Debug(arg string) error {
	switch arg {
	case "delve":
		mg.Deps(CreateDelveCompose, mg.F(LocalDev, "debug"))

		err := os.MkdirAll("./delve", os.ModePerm)
		if err != nil {
			return err
		}

		os.Setenv("ARMADA_IMAGE", imageName)
		os.Setenv("COMPOSE_FILE", "docker-compose.dev.yaml")

		err = StartComponents()
		if err != nil {
			return err
		}

		fmt.Println("Please give up to 5 minutes for compilation.")
	case "vscode":
		mg.Deps(StopComponents)
		mg.Deps(mg.F(LocalDev, "debug"))

		fmt.Println("\nPlease now use the following launch.json configuration in VSCode:")
		fmt.Println("./developer/debug/launch.json")
		_, err := os.Stat("./.vscode/launch.json")
		if os.IsNotExist(err) {
			fmt.Println("\nCreating launch.json file automatically...")
			err = os.MkdirAll("./.vscode", os.ModePerm)
			if err != nil {
				return err
			}
			bytes, err := os.ReadFile("./developer/debug/launch.json")
			if err != nil {
				return err
			}
			err = os.WriteFile("./.vscode/launch.json", bytes, os.ModePerm)
			if err != nil {
				return err
			}
		}

	case "local":
		mg.Deps(StopComponents)
		mg.Deps(mg.F(LocalDev, "debug"))

		fmt.Println("\nPlease see the developer docs for more info: ./docs/developer.md")

	default:
		fmt.Println("Invalid argument. Please use 'delve' to debug.")
	}

	return nil
}
