package main

import (
	"github.com/G-Research/k8s-batch/internal/executor/startup"
)

func main() {
	configuration := startup.LoadConfiguration()
	startup.StartUp(configuration)
}
