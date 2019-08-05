REPO_ROOT=$(git rev-parse --show-toplevel)

echo "Setting up cluster1"
kind create cluster --config ${REPO_ROOT}/test/end_to_end/setup/kind/worker-master-config.yaml --name cluster1 --wait 2m
kind load --name cluster1 docker-image armada-executor:b91e651cba4ed065cbbbea5bf43ef688b40bd766
KUBECONFIG=$(kind get kubeconfig-path --name=cluster1) helm template ./executor --name=armada-executor --namespace=armada | kubectl apply -f -