REPO_ROOT=$(git rev-parse --show-toplevel)

echo "Setting up cluster1"
kind create cluster --config ${REPO_ROOT}/e2e/setup/worker-master-config.yaml --name cluster1 --wait 3m
kind load --name cluster1 docker-image ${ECR_REPOSITORY}/armada-executor:b91e651cba4ed065cbbbea5bf43ef688b40bd766
export KUBECONFIG=$(kind get kubeconfig-path --name=cluster1)
kubectl create ns armada
helm template ${REPO_ROOT}/deployment/executor --name=armada-executor --namespace=armada --set image.repository=${ECR_REPOSITORY}/armada-executor --set image.tag=b91e651cba4ed065cbbbea5bf43ef688b40bd766 | kubectl apply -f -
