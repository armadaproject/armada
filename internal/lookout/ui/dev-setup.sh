#!/bin/sh

kind create cluster --name demo-a --config ./internal/lookout/ui/kind-config.yaml

docker run -d --name redis -p 6379:6379 redis
docker run -d --name nats -p 4223:4223 -p 8223:8223 nats-streaming -p 4223 -m 8223
docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=psw postgres

go run ./cmd/lookout/main.go --migrateDatabase

echo 'go run ./cmd/armada/main.go --config ./e2e/setup/insecure-armada-auth-config.yaml --config ./e2e/setup/nats/armada-config.yaml'
echo 'go run ./cmd/lookout/main.go'
echo 'ARMADA_APPLICATION_CLUSTERID=demo-a ARMADA_METRIC_PORT=9001 go run ./cmd/executor/main.go'
