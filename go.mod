module github.com/armadaproject/armada

go 1.23

toolchain go1.23.1

// athenz@v1.10.5 and onwards bundle encrypted signing keys with the source code.
// Because corporate proxies may block go get commands that pull in encrypted data,
// we replace athenz@v1.10.5 or later with athenz@v1.10.4
replace github.com/AthenZ/athenz v1.10.39 => github.com/AthenZ/athenz v1.10.4

require (
	github.com/apache/pulsar-client-go v0.14.0
	github.com/coreos/go-oidc v2.2.1+incompatible
	github.com/go-openapi/analysis v0.22.2
	github.com/go-openapi/jsonreference v0.20.4
	github.com/go-openapi/loads v0.21.5
	github.com/go-openapi/runtime v0.26.0
	github.com/go-openapi/spec v0.20.14
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.4
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/uuid v1.6.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/hashicorp/go-memdb v1.3.4
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/golang-lru v1.0.2
	github.com/jolestar/go-commons-pool v2.0.0+incompatible
	github.com/jstemmer/go-junit-report/v2 v2.1.0
	github.com/mattn/go-zglob v0.0.6
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/mapstructure v1.5.0
	github.com/oklog/ulid v1.3.1
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.20.5
	github.com/renstrom/shortuuid v3.0.0+incompatible
	github.com/spf13/cobra v1.8.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.19.0
	github.com/stretchr/testify v1.9.0
	golang.org/x/exp v0.0.0-20241009180824-f66d83c29e7c
	golang.org/x/net v0.30.0
	golang.org/x/oauth2 v0.23.0
	golang.org/x/sync v0.8.0
	google.golang.org/genproto v0.0.0-20240213162025-012b6fc9bca9 // indirect
	google.golang.org/grpc v1.67.1
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.26.15
	k8s.io/apimachinery v0.26.15
	k8s.io/client-go v0.26.15
	k8s.io/component-helpers v0.26.15
	k8s.io/kubectl v0.26.15
	k8s.io/kubelet v0.26.15
	k8s.io/utils v0.0.0-20240502163921-fe8a2dddb1d0
	sigs.k8s.io/yaml v1.4.0
)

require (
	github.com/IBM/pgxpoolprometheus v1.1.1
	github.com/Masterminds/semver/v3 v3.2.1
	github.com/benbjohnson/immutable v0.4.3
	github.com/charmbracelet/glamour v0.7.0
	github.com/go-openapi/errors v0.21.0
	github.com/go-openapi/strfmt v0.21.10
	github.com/go-openapi/swag v0.22.6
	github.com/go-openapi/validate v0.22.6
	github.com/go-playground/validator/v10 v10.15.4
	github.com/gogo/status v1.1.1
	github.com/goreleaser/goreleaser v1.24.0
	github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus v1.0.1
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.2.0
	github.com/jackc/pgx/v5 v5.5.4
	github.com/jessevdk/go-flags v1.5.0
	github.com/magefile/mage v1.14.0
	github.com/minio/highwayhash v1.0.2
	github.com/openconfig/goyang v1.2.0
	github.com/prometheus/common v0.60.0
	github.com/redis/go-redis/extra/redisprometheus/v9 v9.0.5
	github.com/redis/go-redis/v9 v9.7.0
	github.com/segmentio/fasthash v1.0.3
	github.com/sirupsen/logrus v1.9.3
	github.com/xitongsys/parquet-go v1.6.2
	go.uber.org/mock v0.5.0
	go.uber.org/zap v1.27.0
	golang.org/x/term v0.25.0
	golang.org/x/time v0.5.0
	google.golang.org/genproto/googleapis/api v0.0.0-20240814211410-ddb44dafa142
	gopkg.in/inf.v0 v0.9.1
)

require (
	github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4 // indirect
	github.com/99designs/keyring v1.2.1 // indirect
	github.com/AthenZ/athenz v1.10.39 // indirect
	github.com/DataDog/zstd v1.5.0 // indirect
	github.com/alecthomas/chroma/v2 v2.8.0 // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20200730104253-651201b0f516 // indirect
	github.com/apache/thrift v0.14.2 // indirect
	github.com/ardielle/ardielle-go v1.5.2 // indirect
	github.com/asaskevich/govalidator v0.0.0-20230301143203-a9d515a09cc2 // indirect
	github.com/aymanbagabas/go-osc52/v2 v2.0.1 // indirect
	github.com/aymerick/douceur v0.2.0 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.4.0 // indirect
	github.com/buger/jsonparser v1.1.1 // indirect
	github.com/caarlos0/log v0.4.4 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/charmbracelet/lipgloss v0.9.1 // indirect
	github.com/danieljoos/wincred v1.1.2 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dlclark/regexp2 v1.4.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dvsekhvalnov/jose2go v1.6.0 // indirect
	github.com/elliotchance/orderedmap/v2 v2.2.0 // indirect
	github.com/emicklei/go-restful/v3 v3.9.0 // indirect
	github.com/evanphx/json-patch v4.12.0+incompatible // indirect
	github.com/fatih/camelcase v1.0.0 // indirect
	github.com/fortytw2/leaktest v1.3.0 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.2 // indirect
	github.com/go-errors/errors v1.0.1 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-openapi/jsonpointer v0.20.2 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2 // indirect
	github.com/gogo/googleapis v0.0.0-20180223154316-0cd9801be74a // indirect
	github.com/golang-jwt/jwt/v5 v5.2.1 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.0.1 // indirect
	github.com/google/gnostic v0.5.7-v3refs // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/goreleaser/fileglob v1.3.0 // indirect
	github.com/goreleaser/nfpm/v2 v2.35.3 // indirect
	github.com/gorilla/css v1.0.0 // indirect
	github.com/gregjones/httpcache v0.0.0-20180305231024-9cad4c3443a7 // indirect
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c // indirect
	github.com/hamba/avro/v2 v2.22.2-0.20240625062549-66aad10411d9 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/hcl v1.0.1-vault-5 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/invopop/jsonschema v0.12.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/leodido/go-urn v1.2.4 // indirect
	github.com/liggitt/tabwriter v0.0.0-20181228230101-89fcab3d43de // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-isatty v0.0.18 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/microcosm-cc/bluemonday v1.0.25 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/monochromegane/go-gitignore v0.0.0-20200626010858-205db1a8cc00 // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/muesli/reflow v0.3.0 // indirect
	github.com/muesli/termenv v0.15.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pelletier/go-toml/v2 v2.2.3 // indirect
	github.com/peterbourgon/diskv v2.0.1+incompatible // indirect
	github.com/pierrec/lz4 v2.0.5+incompatible // indirect
	github.com/pierrec/lz4/v4 v4.1.8 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/pquerna/cachecontrol v0.2.0 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rivo/uniseg v0.4.2 // indirect
	github.com/sagikazarmark/locafero v0.6.0 // indirect
	github.com/sagikazarmark/slog-shim v0.1.0 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/spf13/cast v1.7.0 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/wk8/go-ordered-map/v2 v2.1.8 // indirect
	github.com/xitongsys/parquet-go-source v0.0.0-20200817004010-026bad9b25d0 // indirect
	github.com/xlab/treeprint v1.1.0 // indirect
	github.com/yuin/goldmark v1.5.4 // indirect
	github.com/yuin/goldmark-emoji v1.0.2 // indirect
	go.mongodb.org/mongo-driver v1.13.1 // indirect
	go.starlark.net v0.0.0-20200306205701-8dd3e2ee1dd5 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.28.0 // indirect
	golang.org/x/mod v0.21.0 // indirect
	golang.org/x/sys v0.26.0 // indirect
	golang.org/x/text v0.19.0 // indirect
	golang.org/x/xerrors v0.0.0-20231012003039-104605ab7028 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241015192408-796eee8c2d53 // indirect
	google.golang.org/protobuf v1.35.1 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/square/go-jose.v2 v2.6.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/cli-runtime v0.26.15 // indirect
	k8s.io/klog/v2 v2.100.1 // indirect
	k8s.io/kube-openapi v0.0.0-20221012153701-172d655c2280 // indirect
	sigs.k8s.io/json v0.0.0-20220713155537-f223a00ba0e2 // indirect
	sigs.k8s.io/kustomize/api v0.12.1 // indirect
	sigs.k8s.io/kustomize/kyaml v0.13.9 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.3 // indirect
)
