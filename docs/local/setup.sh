#!/bin/sh -ex

KIND_IMG="kindest/node:v1.21.10"
CHART_VERSION_ARMADA="v0.3.20"
CHART_VERSION_ARMADA_EXECUTOR_MONITORING="v0.1.9"
CHART_VERSION_KUBE_PROMETHEUS_STACK="13.0.0"
CHART_VERSION_NATS="0.13.0"
CHART_VERSION_POSTGRES="11.9.12"
CHART_VERSION_PULSAR="2.9.4"
CHART_VERSION_REDIS="4.22.3"

printf "\n*******************************************************\n"
printf "Running script which will deploy a local Armada cluster"
printf "\n*******************************************************\n"

#####################################################
#                HELM CONFIGURATION                 #
#####################################################
printf "\n*******************************************************\n"
printf "Registering required helm repositories ..."
printf "\n*******************************************************\n"
helm repo add dandydev https://dandydeveloper.github.io/charts
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add nats https://nats-io.github.io/k8s/helm/charts
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add gresearch https://g-research.github.io/charts
helm repo add apache https://pulsar.apache.org/charts
helm repo update

#####################################################
#                  ARMADA SERVER                    #
#####################################################
printf "\n*******************************************************\n"
printf "Deploying Armada server ..."
printf "\n*******************************************************\n"
if kind delete cluster --name quickstart-armada-server; then
  printf "Deleting existing quickstart-armada-server ..."

fi
kind create cluster --name quickstart-armada-server --config ./docs/quickstart/kind/kind-config-server.yaml --image $KIND_IMG

# Set cluster as current context
kind export kubeconfig --name=quickstart-armada-server

# Install Redis
printf "\nStarting Redis ...\n"
helm install redis dandydev/redis-ha --version $CHART_VERSION_REDIS -f docs/quickstart/helm/values-redis.yaml

# Install nats-streaming
printf "\nStarting NATS ...\n"
helm install nats nats/stan --version $CHART_VERSION_NATS --wait

# Install Apache Pulsar
printf "\nStarting Pulsar ...\n"
helm install pulsar apache/pulsar --version $CHART_VERSION_PULSAR -f docs/quickstart/helm/values-pulsar.yaml

# Install Prometheus
printf "\nStarting Prometheus ...\n"
helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack --version $CHART_VERSION_KUBE_PROMETHEUS_STACK -f docs/quickstart/helm/values-server-prometheus.yaml

# Install Armada server
printf "\nStarting Armada server ...\n"
helm install armada-server gresearch/armada --version $CHART_VERSION_ARMADA -f ./docs/quickstart/helm/values-server.yaml

# Get server IP for executors
SERVER_IP=$(kubectl get nodes quickstart-armada-server-worker -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
#####################################################

#####################################################
#               ARMADA EXECUTOR 1                   #
#####################################################
printf "\n*******************************************************\n"
printf "Deploying first Armada executor cluster ..."
printf "\n*******************************************************\n"
if kind delete cluster --name quickstart-armada-executor-0; then
  printf "Deleting existing quickstart-armada-executor-0  cluster ..."

fi
kind create cluster --name quickstart-armada-executor-0 --config ./docs/quickstart/kind/kind-config-executor.yaml --image $KIND_IMG

# Set cluster as current context
kind export kubeconfig --name=quickstart-armada-executor-0

# Install Prometheus
helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack --version $CHART_VERSION_KUBE_PROMETHEUS_STACK -f docs/quickstart/helm/values-executor-prometheus.yaml

# Install executor
helm install armada-executor gresearch/armada-executor --version $CHART_VERSION_ARMADA --set applicationConfig.apiConnection.armadaUrl="$SERVER_IP:30000" -f docs/quickstart/helm/values-executor.yaml
helm install armada-executor-cluster-monitoring gresearch/executor-cluster-monitoring --version $CHART_VERSION_ARMADA_EXECUTOR_MONITORING -f docs/quickstart/helm/values-executor-cluster-monitoring.yaml

# Get executor IP for Grafana
EXECUTOR_0_IP=$(kubectl get nodes quickstart-armada-executor-0-worker -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
#####################################################

#####################################################
#               ARMADA EXECUTOR 2                   #
#####################################################
printf "\n*******************************************************\n"
printf "Deploying second Armada executor cluster ..."
printf "\n*******************************************************\n"
if kind delete cluster --name quickstart-armada-executor-1; then
  printf "Deleting existing quickstart-armada-executor-1  cluster ..."

fi
kind create cluster --name quickstart-armada-executor-1 --config ./docs/quickstart/kind/kind-config-executor.yaml --image $KIND_IMG

# Set cluster as current context
kind export kubeconfig --name=quickstart-armada-executor-1

# Install Prometheus
helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack --version $CHART_VERSION_KUBE_PROMETHEUS_STACK -f docs/quickstart/helm/values-executor-prometheus.yaml

# Install executor
helm install armada-executor gresearch/armada-executor --version $CHART_VERSION_ARMADA --set applicationConfig.apiConnection.armadaUrl="$SERVER_IP:30000" -f docs/quickstart/helm/values-executor.yaml
helm install armada-executor-cluster-monitoring gresearch/executor-cluster-monitoring --version $CHART_VERSION_ARMADA_EXECUTOR_MONITORING -f docs/quickstart/helm/values-executor-cluster-monitoring.yaml

# Get executor IP for Grafana
EXECUTOR_1_IP=$(kubectl get nodes quickstart-armada-executor-1-worker -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
#####################################################

#####################################################
#                 ARMADA LOOKOUT                    #
#####################################################
printf "\n*******************************************************\n"
printf "Deploying Armada Lookout UI ..."
printf "\n*******************************************************\n"
kind export kubeconfig --name=quickstart-armada-server

# Install postgres
helm install postgres bitnami/postgresql --version $CHART_VERSION_POSTGRES --wait --set auth.postgresPassword=psw

# Run database migration
helm install lookout-migration gresearch/armada-lookout-migration --version $CHART_VERSION_ARMADA --wait -f docs/quickstart/helm/values-lookout.yaml

# Install Armada Lookout
helm install lookout gresearch/armada-lookout --version $CHART_VERSION_ARMADA -f docs/quickstart/helm/values-lookout.yaml
#####################################################

#####################################################
#                 GRAFANA CONFIG                    #
#####################################################
printf "\n*******************************************************\n"
printf "Configuring Grafana dashboard for Armada ..."
printf "\n*******************************************************\n"
curl -X POST -i http://admin:prom-operator@localhost:30001/api/datasources -H "Content-Type: application/json" -d '{"name":"cluster-0","type":"prometheus","url":"http://'$EXECUTOR_0_IP':30001","access":"proxy","basicAuth":false}'
curl -X POST -i http://admin:prom-operator@localhost:30001/api/datasources -H "Content-Type: application/json" -d '{"name":"cluster-1","type":"prometheus","url":"http://'$EXECUTOR_1_IP':30001","access":"proxy","basicAuth":false}'
curl -X POST -i http://admin:prom-operator@localhost:30001/api/dashboards/import --data-binary @./docs/quickstart/grafana-armada-dashboard.json -H "Content-Type: application/json"

printf "\n*******************************************************\n"
printf "Finished deploying local Armada cluster"
printf "\n*******************************************************\n"

bs="\033[1m"
be="\033[0m"
printf "\nArmada Lookout UI can be accessed by doing the following:"
printf "\n\t* type %bkubectl port-forward svc/armada-lookout 8080:8080%b in your terminal" "$bs" "$be"
printf "\n\t* open %bhttp://localhost:8080%b in your browser\n" "$bs" "$be"

printf "\nArmada uses Grafana for monitoring, do the following in order to access it:"
printf "\n\t* type %bkubectl port-forward svc/kube-prometheus-stack-grafana 8081:80%b in your terminal" "$bs" "$be"
printf "\n\t* open %bhttp://localhost:8081%b in your browser" "$bs" "$be"
printf "\n\t* use %badmin:prom-operator%b as default admin credentials for login" "$bs" "$be"
printf "\n\t* open the %bArmada - Overview%b dashboard\n" "$bs" "$be"

./docs/local/armadactl.sh