# Armada API

Armada exposes an API via gRPC or REST.

## gRPC
The API is defined in `/pkg/api` folder with `*.proto` files as source for all generated code. 

Folder `/pkg/api` also contains generated clients and together with helper methods from `/pkg/client` provides a convenient way to call Armada API from go code. See armadactl code for [examples](../cmd/armadactl/cmd/submit.go).

### Public API

Following subset of API defined in `/pkg/api` is intended for public use.

#### api.Submit ([definition](../pkg/api/submit.proto))
 
__/api.Submit/SubmitJobs__ - submitting jobs to be run

__/api.Submit/CancelJobs__ - cancel jobs

__/api.Submit/CreateQueue__ - create or update existing queue

__/api.Submit/DeleteQueue__ - remove queue

__/api.Submit/GetQueueInfo__ - get information about active queue jobs

#### api.Event  ([definition](../pkg/api/submit.proto))

__/api.Event/GetJobSetEvents__ - read events of jobs running under particular JobSet


### Internal
There are additional API methods defined in proto specifications, which are used by Armada executor and not intended to be used by external users. This API can change in any version.

- [event.proto](../pkg/api/event.proto) - methods for event reporting
- [queue.proto](../pkg/api/queue.proto) - methods related to job leasing by executor
- [usage.proto](../pkg/api/usage.proto) - methods for reporting of resources usage

## REST
The REST API only exposes the public part of the gRPC API and it is implemented using grpc-gateway (https://github.com/grpc-ecosystem/grpc-gateway).

Swagger json specification can be found [here](../pkg/api/api.swagger.json) and is also served by armada under `my.armada.deployment/api/swagger.json`

## Authentication

Both gRPC and REST API support the same set of authentication methods. In the case of gRPC all authentication methods uses `authorization` key in grpc metadata. The REST API use standard http Authorization header (which is translated by grpc-gateway to `authorization` metadata).

See helm chart [documentation](./helm/server.md#Authentication) for different server authentication schemes setup.

### No Auth
For testing, armada can be configured to accept no authentication. All operations will use user `anonymous` in this case.

### OpenId Authentication
When server is configured with OpenID, it will accept authorization header or metadata in the form `Bearer {oauth_token}`.

### Basic Authentication
For basic authentication API accepts standard authorization header or metadata in the form `basic {base64(user:password)}`.

### Kerberos
For Kerberos authentication API accepts the same authorization metadata for gRPC as standard Kerberos http SPNEGO authorization headers, the API responds with `WWW-Authenticate` header or metadata.
