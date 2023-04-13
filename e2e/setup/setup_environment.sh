REPO_ROOT=$(git rev-parse --show-toplevel)

echo "Installing kubectl"
curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.15.0/bin/linux/amd64/kubectl
chmod +x ./kubectl
sudo mv ./kubectl /usr/local/bin/kubectl

echo ">>> Installing Helm"
curl -L https://get.helm.sh/helm-v2.14.3-linux-amd64.tar.gz > helm.tar.gz
tar -zxvf helm.tar.gz
sudo mv linux-amd64/helm /usr/local/bin/helm

echo "Installing Go"
sudo rm -rf /usr/local/go
wget https://dl.google.com/go/go1.20.2.linux-amd64.tar.gz
sudo tar -xvf go1.20.2.linux-amd64.tar.gz
sudo mv go /usr/local
go version
