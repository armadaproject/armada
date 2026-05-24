# Slurm Integration Demo

This guide walks through setting up a local two-cluster demo of Armada routing jobs to a
Slurm cluster via slurm-bridge. At the end you will have:

- An **armada-server** kind cluster running the full Armada control plane
- A **slurm-executor** kind cluster running Slurm, slurm-bridge, and the Armada executor

## Prerequisites

- [kind](https://kind.sigs.k8s.io/) ≥ 0.24
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [helm](https://helm.sh/) ≥ 3.14
- [Docker](https://www.docker.com/) (with BuildKit)
- [armadactl](https://github.com/armadaproject/armada/releases) — download the binary for
  your platform from the Armada releases page
- Go ≥ 1.22 (only needed if building the Armada scheduler image from this branch)

Both kind clusters run on the same Docker bridge network, so they can communicate via
container IP addresses without any extra networking setup.

## 1. Create the armada-server cluster

```bash
kind create cluster --name armada-server
```

Deploy the Armada control plane using
[armada-operator](https://github.com/armadaproject/armada-operator). The operator manages
Pulsar, Postgres, and all Armada components via CRDs. See the operator's quickstart for
full details; the key points for this demo are:

- Use the scheduler image built from this branch (the `SkipNodeBinding` change is not yet
  in the upstream release)
- Configure the `slurm` pool with `skipNodeBinding: true` in the scheduler's
  `applicationConfig`:

```yaml
scheduling:
  pools:
    - name: slurm
      skipNodeBinding: true
```

- Expose the scheduler's gRPC port (50051) as a NodePort so the executor cluster can reach
  it. The armada-server control-plane container IP (visible via `kubectl get node
  armada-server-control-plane -o wide`) combined with the NodePort gives the address the
  executor will connect to.

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: armada-scheduler-nodeport
  namespace: armada
spec:
  type: NodePort
  selector:
    app: armada-scheduler
  ports:
    - name: grpc
      port: 50051
      targetPort: 50051
      nodePort: 30051
EOF
```

## 2. Create the slurm-executor cluster

The slurm-executor cluster requires Kubernetes ≥ 1.35 with DRA feature gates and CDI
enabled. Two of its worker nodes need the slurm-bridge label and taint so that Slurm and
slurm-bridge know which nodes they manage.

Create a kind config file:

```yaml
# kind-slurm-executor.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
featureGates:
  DynamicResourceAllocation: true
  DRAExtendedResource: true
  DRAResourceClaimDeviceStatus: true
containerdConfigPatches:
  - |-
    [plugins."io.containerd.grpc.v1.cri"]
      enable_cdi = true
nodes:
  - role: control-plane
  - role: worker
  - role: worker
  - role: worker
    labels:
      scheduler.slinky.slurm.net/slurm-bridge: worker
    kubeadmConfigPatches:
      - |
        kind: JoinConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            register-with-taints: "slinky.slurm.net/managed-node=slurm-bridge-scheduler:NoExecute"
  - role: worker
    labels:
      scheduler.slinky.slurm.net/slurm-bridge: worker
    kubeadmConfigPatches:
      - |
        kind: JoinConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            register-with-taints: "slinky.slurm.net/managed-node=slurm-bridge-scheduler:NoExecute"
```

```bash
kind create cluster --name slurm-executor --config kind-slurm-executor.yaml --image kindest/node:v1.35.1
kubectl config use-context kind-slurm-executor
```

## 3. Install the slurm-executor dependency stack

Install in this order. Each `--wait` ensures the next step has its dependencies ready.

```bash
# scheduler-plugins (CoScheduling, required by slurm-bridge)
helm upgrade --install scheduler-plugins scheduler-plugins \
  --repo https://scheduler-plugins.sigs.k8s.io \
  --namespace scheduler-plugins --create-namespace \
  --set plugins.enabled='{CoScheduling}' \
  --set scheduler.replicaCount=1 --wait

# JobSet
helm upgrade --install jobset \
  oci://registry.k8s.io/jobset/charts/jobset \
  --namespace jobset-system --create-namespace --wait

# LeaderWorkerSet
helm upgrade --install lws \
  oci://registry.k8s.io/lws/charts/lws \
  --namespace lws-system --create-namespace --wait

# cert-manager (required by slurm-operator and slurm-bridge admission webhook)
helm upgrade --install cert-manager \
  oci://quay.io/jetstack/charts/cert-manager \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true --wait

# slurm-operator CRDs and operator
helm upgrade --install slurm-operator-crds \
  oci://ghcr.io/slinkyproject/charts/slurm-operator-crds \
  --namespace slurm --create-namespace --wait
helm upgrade --install slurm-operator \
  oci://ghcr.io/slinkyproject/charts/slurm-operator \
  --namespace slurm --wait
```

## 4. Deploy the Slurm cluster

The slurm chart deploys slurmctld, slurmd (as a DaemonSet on the labeled nodes), and the
Slurm REST API. The `slurm-bridge` nodeset targets the two nodes labeled in step 2.

```bash
cat > /tmp/slurm-values.yaml <<EOF
nodesets:
  slinky:
    enabled: false
  slurm-bridge:
    enabled: true
    scalingMode: DaemonSet
    slurmd:
      image:
        repository: ghcr.io/slinkyproject/slurmd
        tag: 25.11-ubuntu24.04
    partition:
      enabled: true
    podSpec:
      nodeSelector:
        scheduler.slinky.slurm.net/slurm-bridge: worker
        kubernetes.io/os: linux
      tolerations:
        - key: slinky.slurm.net/managed-node
          operator: Equal
          value: slurm-bridge-scheduler
          effect: NoExecute
EOF

helm upgrade --install slurm \
  oci://ghcr.io/slinkyproject/charts/slurm \
  --version 1.1.0 \
  --namespace slurm --create-namespace \
  -f /tmp/slurm-values.yaml --wait --timeout=10m
```

Verify the `slurm-bridge` partition is up and both nodes are idle:

```bash
kubectl exec -n slurm slurm-controller-0 -- sinfo
# PARTITION    AVAIL  TIMELIMIT  NODES  STATE NODELIST
# slurm-bridge    up   infinite      2   idle ...
```

## 5. Build and deploy slurm-bridge

slurm-bridge images are not yet published to a public registry and must be built from
source.

```bash
git clone --depth=1 https://github.com/SlinkyProject/slurm-bridge.git
cd slurm-bridge

# Build the three component images
docker build --target scheduler  -t slurm-bridge-scheduler:dev  .
docker build --target admission  -t slurm-bridge-admission:dev  .
docker build --target controllers -t slurm-bridge-controllers:dev .

# Load into the kind cluster
kind load docker-image slurm-bridge-scheduler:dev  --name slurm-executor
kind load docker-image slurm-bridge-admission:dev  --name slurm-executor
kind load docker-image slurm-bridge-controllers:dev --name slurm-executor

# Create the namespace and JWT token resource slurm-bridge needs
kubectl create namespace slinky
kubectl apply -f hack/token.yaml

# Install
helm upgrade --install slurm-bridge helm/slurm-bridge \
  --namespace slinky \
  --set scheduler.image.repository=slurm-bridge-scheduler \
  --set scheduler.image.tag=dev \
  --set scheduler.image.pullPolicy=Never \
  --set admission.image.repository=slurm-bridge-admission \
  --set admission.image.tag=dev \
  --set admission.image.pullPolicy=Never \
  --set controllers.image.repository=slurm-bridge-controllers \
  --set controllers.image.tag=dev \
  --set controllers.image.pullPolicy=Never \
  --set schedulerConfig.partition=slurm-bridge \
  --wait
```

## 6. Deploy the Armada executor

Install the armada-operator on the slurm-executor cluster (only the operator — no other
Armada CRs), then create an `Executor` CR pointing at the armada-scheduler NodePort.

Replace `<ARMADA_SCHEDULER_IP>` with the internal IP of the armada-server control-plane
node (`kubectl --context kind-armada-server get node armada-server-control-plane -o wide`).

```bash
helm upgrade --install armada-operator \
  /path/to/armada-operator/charts/armada-operator \
  --namespace armada --create-namespace --wait

kubectl apply -f - <<EOF
apiVersion: install.armadaproject.io/v1alpha1
kind: Executor
metadata:
  name: armada-executor
  namespace: armada
spec:
  replicas: 1
  image:
    repository: gresearch/armada-executor
    tag: latest
  applicationConfig:
    application:
      clusterId: slurm-executor
      pool: slurm
    executorApiConnection:
      armadaUrl: <ARMADA_SCHEDULER_IP>:30051
      forceNoTls: true
    metric:
      port: 9001
    kubernetes:
      podDefaults:
        schedulerName: slurm-bridge-scheduler
      minimumPodAge: 0s
      failedPodExpiry: 10m
EOF
```

The executor needs the `armada-default` PriorityClass to exist on the cluster:

```bash
kubectl apply -f - <<EOF
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: armada-default
value: 1000
preemptionPolicy: PreemptLowerPriority
globalDefault: false
EOF
```

Wait for the executor to connect — you should see it reporting free capacity:

```bash
kubectl logs -n armada deployment/armada-executor -f | grep job_requester
# Reporting current free resource cpu: ..., memory: ... Requesting 100 new jobs.
```

## 7. Submit a test job

Configure `armadactl` to point at the armada-server API (NodePort 30002 on the
armada-server control-plane IP), create a queue, and submit a job:

```bash
cat > ~/.armadactl.yaml <<EOF
currentContext: demo
contexts:
  demo:
    armadaUrl: <ARMADA_SERVER_IP>:30002
    forceNoTls: true
EOF

armadactl create queue demo --priority-factor 1

cat > test-job.yaml <<EOF
queue: demo
jobSetId: slurm-test-1
jobs:
  - priority: 1
    namespace: default
    podSpec:
      containers:
        - name: test
          image: busybox:latest
          command: ["sh", "-c", "echo hello from slurm && sleep 30"]
          resources:
            requests:
              cpu: 100m
              memory: 64Mi
            limits:
              cpu: 100m
              memory: 64Mi
    annotations:
      armadaproject.io/pool: slurm
EOF

armadactl submit test-job.yaml
```

Watch the pod appear on the slurm-executor cluster, scheduled by slurm-bridge onto one of
the Slurm-managed nodes, with no `armadaproject.io/nodeId` nodeSelector:

```bash
kubectl --context kind-slurm-executor get pods -n default -o wide
kubectl --context kind-slurm-executor get pod <pod-name> -n default \
  -o jsonpath='{.spec.schedulerName}{"\n"}{.spec.nodeSelector}'
# slurm-bridge-scheduler
# {}
```
