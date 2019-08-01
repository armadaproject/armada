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

Cobra
```
go get -u github.com/spf13/cobra/cobra
```
