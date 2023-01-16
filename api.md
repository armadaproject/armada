# Armada API

Armada exposes an API via gRPC or REST.

## gRPC
The API is defined in `/pkg/api` folder with `*.proto` files as source for all generated code. 

Folder `/pkg/api` also contains generated clients and together with helper methods from `/pkg/client` provides a convenient way to call Armada API from go code. See armadactl code for
[examples](https://github.com/armadaproject/armada/blob/master/cmd/armadactl/cmd/submit.go).

Following subset of API defined in `/pkg/api` is intended for public use.

### api.Submit ([definition](https://github.com/armadaproject/armada/blob/master/pkg/api/submit.proto))
 
__/api.Submit/SubmitJobs__ - submitting jobs to be run

__/api.Submit/CancelJobs__ - cancel jobs

__/api.Submit/CreateQueue__ - create a new queue

__/api.Submit/UpdateQueue__ - update an existing queue

__/api.Submit/DeleteQueue__ - remove queue

__/api.Submit/GetQueue__ - get information about queue (name, permissions)

__/api.Submit/GetQueueInfo__ - get information about queued (active jobs, including those currently running)

### api.Event  ([definition](https://github.com/armadaproject/armada/blob/master/pkg/api/submit.proto))

__/api.Event/GetJobSetEvents__ - read events of jobs running under particular JobSet


### Internal
There are additional API methods defined in proto specifications, which are used by Armada executor and not intended to be used by external users. This API can change in any version.

- [event.proto](https://github.com/armadaproject/armada/blob/master/pkg/api/event.proto) - methods for event reporting
- [queue.proto](https://github.com/armadaproject/armada/blob/master/pkg/api/queue.proto) - methods related to job leasing by executor
- [usage.proto](https://github.com/armadaproject/armada/blob/master/pkg/api/usage.proto) - methods for reporting of resources usage

## REST
The REST API only exposes the public part of the gRPC API and it is implemented using [grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway).

Swagger json specification can be found [here](https://github.com/armadaproject/armada/blob/master/pkg/api/api.swagger.json) and is also served by Armada under `my.armada.deployment/api/swagger.json`

## Authentication

Both gRPC and REST API support the same set of authentication methods. In the case of gRPC all authentication methods uses `authorization` key in grpc metadata. The REST API use standard http Authorization header (which is translated by grpc-gateway to `authorization` metadata).

See helm chart [documentation](https://armadaproject.io/helm#Authentication) for different server authentication schemes setup.

### No Auth
For testing, Armada can be configured to accept no authentication. All operations will use user `anonymous` in this case.

### OpenId Authentication
When server is configured with OpenID, it will accept authorization header or metadata in the form `Bearer {oauth_token}`.

### Basic Authentication
For basic authentication API accepts standard authorization header or metadata in the form `basic {base64(user:password)}`.

### Kerberos
For Kerberos authentication API accepts the same authorization metadata for gRPC as standard Kerberos http SPNEGO authorization headers, the API responds with `WWW-Authenticate` header or metadata.


## Permissions

Armada will determine which actions you are able to perform based on your user's permissions.
These are defined as global or on a per queue basis.

Below is the list of global Armada permissions (defined [here](https://github.com/armadaproject/armada/blob/master/internal/armada/permissions/permissions.go)):
* `submit_jobs`
* `submit_any_jobs`
* `create_queue`
* `delete_queue`
* `cancel_jobs`
* `cancel_any_jobs`
* `reprioritize_jobs`
* `reprioritize_any_jobs`
* `watch_events`
* `watch_all_events`

In addition, the following queue-specific permission verbs control what actions can be taken per individual queues (defined [here](https://github.com/armadaproject/armada/blob/master/pkg/client/queue/permission_verb.go)):
* `submit`
* `cancel`
* `reprioritize`
* `watch`

The table below shows which permissions are required for a user to access each API endpoint (either directly or via a group).
Note queue-specific permission require a user to be bound to a global permission as well (shown as tuples in the table below).

| Endpoint           | Global Permissions      | Queue Permissions                     |
|--------------------|-------------------------|---------------------------------------|
| `SubmitJobs`       | `submit_any_jobs`       | (`submit_jobs`, `submit`)             |
| `CancelJobs`       | `cancel_any_jobs`       | (`cancel_jobs`, `cancel`)             |
| `ReprioritizeJobs` | `reprioritize_any_jobs` | (`reprioritize_jobs`, `reprioritize`) |
| `CreateQueue`      | `create_queue`          |                                       |
| `UpdateQueue`      | `create_queue`          |                                       |
| `DeleteQueue`      | `delete_queue`          |                                       |
| `GetQueue`         |                         |                                       |
| `GetQueueInfo`     | `watch_all_events`      | (`watch_events`, `watch`)             |
| `GetJobSetEvents`  | `watch_all_events`      | (`watch_events`, `watch`)             |
