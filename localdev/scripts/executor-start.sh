./localdev/scripts/kube-access.sh
ARMADA_APPLICATION_CLUSTERID=demo-a ARMADA_METRIC_PORT=9001 go run ./cmd/executor/main.go --config localdev/config/executor/config.yaml
