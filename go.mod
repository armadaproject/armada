module github.com/G-Research/armada

go 1.16

// athenz@v1.10.5 and onwards bundle encrypted signing keys with the source code.
// Because corporate proxies may block go get commands that pull in encrypted data,
// we replace athenz@v1.10.5 or later with athenz@v1.10.4.
replace github.com/AthenZ/athenz v1.10.39 => github.com/AthenZ/athenz v1.10.4

require (
	github.com/alexbrainman/sspi v0.0.0-20180613141037-e580b900e9f5
	github.com/alicebob/gopher-json v0.0.0-20180125190556-5a6b3ba71ee6 // indirect
	github.com/alicebob/miniredis v2.5.0+incompatible
	github.com/apache/pulsar-client-go v0.8.1-0.20220429133321-5ee63303d43e
	github.com/avast/retry-go v3.0.0+incompatible
	github.com/coreos/go-oidc v2.2.1+incompatible
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f
	github.com/doug-martin/goqu/v9 v9.18.0
	github.com/go-ldap/ldap/v3 v3.4.1
	github.com/go-openapi/analysis v0.21.3
	github.com/go-openapi/jsonreference v0.20.0
	github.com/go-openapi/loads v0.21.1
	github.com/go-openapi/runtime v0.24.1
	github.com/go-openapi/spec v0.20.4
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/go-swagger/go-swagger v0.29.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/gomodule/redigo v2.0.0+incompatible // indirect
	github.com/google/go-cmp v0.5.7
	github.com/google/uuid v1.3.0
	github.com/gordonklaus/ineffassign v0.0.0-20210914165742-4cc7213b9bc8
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/golang-lru v0.5.4
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/instrumenta/kubeval v0.0.0-20190918223246-8d013ec9fc56
	github.com/jackc/pgconn v1.12.1
	github.com/jackc/pgerrcode v0.0.0-20201024163028-a0d42d470451
	github.com/jackc/pgtype v1.11.0
	github.com/jackc/pgx/v4 v4.16.1
	github.com/jcmturner/gokrb5/v8 v8.4.2-0.20201112171129-78f56934d598
	github.com/jstemmer/go-junit-report v0.9.1
	github.com/jstemmer/go-junit-report/v2 v2.0.0-beta1
	github.com/mattn/go-zglob v0.0.3
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/gox v1.0.1
	github.com/mitchellh/mapstructure v1.4.3
	github.com/nats-io/jsm.go v0.0.26
	github.com/nats-io/nats-server/v2 v2.8.2
	github.com/nats-io/nats-streaming-server v0.24.6
	github.com/nats-io/nats.go v1.16.0
	github.com/nats-io/stan.go v0.10.2
	github.com/oklog/ulid v1.3.1
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.1
	github.com/rakyll/statik v0.1.7
	github.com/renstrom/shortuuid v3.0.0+incompatible
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.4.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.10.1
	github.com/stretchr/testify v1.7.0
	github.com/weaveworks/promrus v1.2.0
	github.com/wlbr/templify v0.0.0-20210816202250-7b8044ca19e9
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	github.com/yuin/gopher-lua v0.0.0-20190514113301-1cd887cd7036 // indirect
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd
	golang.org/x/oauth2 v0.0.0-20211104180415-d3ed0bb246c8
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/tools v0.1.8
	google.golang.org/genproto v0.0.0-20211208223120-3a66f561d7aa
	google.golang.org/grpc v1.43.0
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.22.4
	k8s.io/apimachinery v0.22.4
	k8s.io/client-go v0.22.4
	k8s.io/component-base v0.22.4
	k8s.io/component-helpers v0.22.4
	k8s.io/kubelet v0.22.4
	k8s.io/utils v0.0.0-20220210201930-3a6ce19ff2f9
	sigs.k8s.io/kind v0.14.0
	sigs.k8s.io/yaml v1.3.0
)
