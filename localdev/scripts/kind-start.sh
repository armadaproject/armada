go install sigs.k8s.io/kind@v0.16.0

kind create cluster --config ../e2e/setup/kind.yaml

# Pull all images
docker pull "alpine:3.10"
docker pull "nginx:1.21.6"
docker pull "registry.k8s.io/ingress-nginx/controller:v1.4.0"
docker pull "registry.k8s.io/ingress-nginx/kube-webhook-certgen:v20220916-gd32f8c343"

# Load images into kind
kind load docker-image "alpine:3.10" --name armada-test
kind load docker-image "nginx:1.21.6" --name armada-test
kind load docker-image "registry.k8s.io/ingress-nginx/controller:v1.4.0" --name armada-test
kind load docker-image "registry.k8s.io/ingress-nginx/kube-webhook-certgen:v20220916-gd32f8c343" --name armada-test

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

# Store the kubeconfig in the local directory for containers to access
mkdir -p .kube
kind get kubeconfig --internal --name armada-test > ./.kube/config

# cluster config
kubectl apply -f ../e2e/setup/namespace-with-anonymous-user.yaml --context kind-armada-test
kubectl apply -f ../e2e/setup/priorityclasses.yaml --context kind-armada-test

