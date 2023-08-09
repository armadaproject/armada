REPO_ROOT=$(git rev-parse --show-toplevel)

echo "Setting up cluster1"
kind create cluster --config ${REPO_ROOT}/e2e/setup/worker-master-config.yaml --name cluster1 --wait 3m
kind load --name cluster1 docker-image alpine:3.18
