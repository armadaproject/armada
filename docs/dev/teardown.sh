#!/bin/sh

# Ensure armada, armada-executor, and armada-lookout are not running

kind delete cluster --name demo-a

docker-compose -f ./docs/dev/docker-compose.yaml down
