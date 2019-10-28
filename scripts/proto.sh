protoc -I=./ -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogofaster_out=\
Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,\
Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,plugins=grpc:./ \
./internal/armada/api/*.proto

go run golang.org/x/tools/cmd/goimports -w -local "github.com/G-Research/armada" ./internal/armada/api/