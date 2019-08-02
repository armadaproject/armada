echo "Installing kubectl"
curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.15.0/bin/linux/amd64/kubectl
chmod +x ./kubectl
sudo mv ./kubectl /usr/local/bin/kubectl

echo ">>> Installing Helm"
curl -L https://get.helm.sh/helm-v2.14.3-linux-amd64.tar.gz > helm.tar.gz
tar -zxvf helm.tar.gz
mv linux-amd64/helm /usr/local/bin/helm

echo "Building kind"
docker build -t kind:src . -f ${REPO_ROOT}/test/Dockerfile.kind
docker create -ti --name dummy kind:src sh
docker cp dummy:/go/bin/kind ./kind
docker rm -f dummy

echo "Installing kind"
chmod +x kind
sudo mv kind /usr/local/bin/
kind create cluster --wait 5m
