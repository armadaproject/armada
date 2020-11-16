#!/bin/sh

cd ../../..

kind create cluster --name demoA --config ./example/kind-config.yaml

docker run -d -p 6379:6379 redis
docker run -d --name nats -p 4223:4223 -p 8223:8223 nats-streaming -p 4223 -m 8223
docker run -d --name=postgres -p 5432:5432 -e POSTGRES_PASSWORD=psw postgres

# Run DB creation script
# go run ./cmd/armada/main.go --config ./e2e/setup/nats/armada-config.yaml
# go run ./cmd/lookout/main.go
# KUBECONFIG=$(kind get kubeconfig-path --name="demoA") ARMADA_APPLICATION_CLUSTERID=demoA ARMADA_METRIC_PORT=9001 go run ./cmd/executor/main.go

# go run ./cmd/armadactl/main.go create-queue test --priorityFactor 1
# go run ./cmd/armadactl/main.go submit ./example/jobs.yaml
# go run ./cmd/armadactl/main.go watch test job-set-1
