go install sigs.k8s.io/kind@v0.16.0

kind create cluster --config ../e2e/setup/kind.yaml
# We need an ingress controller to enable cluster ingress
kubectl apply -f ../e2e/setup/ingress-nginx.yaml --context kind-armada-test
# Wait until the ingress controller is ready
echo "Waiting for ingress controller to become ready"
sleep 60 # calling wait immediately can result in "no matching resources found"
kubectl wait --namespace ingress-nginx \
	--for=condition=ready pod \
	--selector=app.kubernetes.io/component=controller \
	--timeout=90s \
	--context kind-armada-test
docker pull "alpine:3.10" # ensure alpine, which is used by tests, is available
docker pull "nginx:1.21.6" # ensure nginx, which is used by tests, is available
kind load docker-image "alpine:3.10" --name armada-test # needed to make alpine available to kind
kind load docker-image "nginx:1.21.6" --name armada-test # needed to make nginx available to kind
mkdir -p .kube
kind get kubeconfig --internal --name armada-test > ./.kube/config
kubectl apply -f ../e2e/setup/namespace-with-anonymous-user.yaml --context kind-armada-test

