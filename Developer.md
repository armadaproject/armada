## Developer setup
Run local redis 
```
sudo docker run --expose=6379 --network=host redis
```

### GRPC
Install protoc (https://github.com/protocolbuffers/protobuf/releases/download/v3.8.0/protoc-3.8.0-linux-x86_64.zip) and make sure it is on $PATH.
Make sure $GOPATH/bin is in your $PATH.

```
go get github.com/gogo/protobuf/proto
go get github.com/gogo/protobuf/protoc-gen-gogofaster
go get github.com/gogo/protobuf/gogoproto
go get google.golang.org/grpc

GO111MODULE=off go get k8s.io/api
GO111MODULE=off go get k8s.io/apimachinery
GO111MODULE=off go get github.com/gogo/protobuf/gogoproto

```

Generate types

```
./scripts/proto.sh
```

you can use cobra cli for adding new armadactl command Cobra 
```
go get -u github.com/spf13/cobra/cobra
cd ./cmd/armadactl
cobra add cmd
```

### Local development with Kind

It is possible to test Armada locally with [kind](https://github.com/kubernetes-sigs/kind) Kubernetes clusters.
```bash
go get sigs.k8s.io/kind
 
# create 2 clusters
kind create cluster --name demoA --config ./example/kind-config.yaml
kind create cluster --name demoB --config ./example/kind-config.yaml 

# run armada
./armada

# run 2 executors
KUBECONFIG=$(kind get kubeconfig-path --name="demoA") ARMADA_APPLICATION_CLUSTERID=demoA ./executor
KUBECONFIG=$(kind get kubeconfig-path --name="demoB") ARMADA_APPLICATION_CLUSTERID=demoB ./executor
```

Depending on your docker setup you might need to load images for jobs you plan to run manually 
```bash
kind load docker-image busybox:latest
```