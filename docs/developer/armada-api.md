# Armada API
<!-- TOC -->- [Armada API](#armada-api)
- [Armada API](#armada-api)
  - [gRPC methods](#grpc-methods)
    - [`api.Submit`](#apisubmit)
    - [`api.Event`](#apievent)
    - [Internal-only methods](#internal-only-methods)
  - [REST API](#rest-api)
  - [Authentication](#authentication)
    - [No authentication](#no-authentication)
    - [OpenID Authentication](#openid-authentication)
    - [Basic Authentication](#basic-authentication)
    - [Kerberos authentication](#kerberos-authentication)
  - [Permissions](#permissions)
    - [Global permissions](#global-permissions)
    - [Queue-specific permissions](#queue-specific-permissions)
    - [User permissions](#user-permissions)

Armada exposes an API via [Google Remote Procedure Call(gRPC)](https://en.wikipedia.org/wiki/GRPC) or REST.

## gRPC methods

The Armada API is defined in the `/pkg/api` folder, with `*.proto` files as the source for all generated code. 

The `/pkg/api` folder also contains generated clients, and together with helper methods from `/pkg/client`, enables you to call the Armada API from go code. For examples, [see the armadactl code](https://github.com/armadaproject/armada/blob/master/cmd/armadactl/cmd/submit.go).

The following API subset defined in `/pkg/api` is intended for public use.

### `api.Submit`
 
* `/api.Submit/SubmitJobs` - submit jobs to be run
* `/api.Submit/CancelJobs` - cancel jobs
* `/api.Submit/CreateQueue` - create a new queue
* `/api.Submit/UpdateQueue` - update an existing queue
* `/api.Submit/DeleteQueue` - remove queue
* `/api.Submit/GetQueue` - get information about queue (name, permissions)
* `/api.Submit/GetQueueInfo` - get information about queued (active jobs, including those currently running)

[Read the `api.Submit` definition](https://github.com/armadaproject/armada/blob/master/pkg/api/submit.proto).

### `api.Event`

* `/api.Event/GetJobSetEvents` - read events of jobs running under a particular `JobSet`

[Read the `api.Event` definition](https://github.com/armadaproject/armada/blob/master/pkg/api/event.proto).

### Internal-only methods

There are additional API methods defined in `proto` specifications, which are used by Armada executor and not intended to be used by external users. This API can change in any version.

* [`event.proto`](https://github.com/armadaproject/armada/blob/master/pkg/api/event.proto) - methods for event reporting
* [`queue.proto`](https://github.com/armadaproject/armada/blob/master/pkg/api/queue.proto) - methods related to job leasing by executor

## REST API

The REST API only exposes the public part of the gRPC API and it is implemented using [`grpc-gateway`](https://github.com/grpc-ecosystem/grpc-gateway).

[Swagger JSON specification](https://github.com/armadaproject/armada/blob/master/pkg/api/api.swagger.json) is also served by Armada under `my.armada.deployment/api/swagger.json`.

## Authentication

Both gRPC and REST API support the same set of authentication methods. In the case of gRPC, all authentication methods use an `authorization` key in gRPC metadata. The REST API uses a standard HTTP authorisation header (which is translated by `grpc-gateway` to `authorization` metadata).

To set up different server authentication schemes, [see the Helm chart documentation](https://armadaproject.io/helm#Authentication).

### No authentication

For testing, Armada can be configured to accept no authentication. In this case, all operations use an `anonymous` user.

### OpenID Authentication

When the server is configured with OpenID, the Armada API accepts the authorisation header or metadata in the form `Bearer {oauth_token}`.

### Basic Authentication

For basic authentication, the Armada API accepts the standard authorisation header or metadata in the form `basic {base64(user:password)}`.

### Kerberos authentication

For Kerberos authentication, the Armada API accepts the same authorisation metadata for gRPC as standard Kerberos HTTP [SPNEGO](https://en.wikipedia.org/wiki/SPNEGO) authorisation headers. The API responds with a `WWW-Authenticate` header or metadata.

## Permissions

Armada will determine which actions you are able to perform, based on your user's permissions.
These are defined as global or on a per-queue basis.

### Global permissions

* `submit_any_jobs`
* `create_queue`
* `delete_queue`
* `cancel_any_jobs`
* `reprioritize_any_jobs`
* `watch_all_events`

[Learn more about global Armada permissions](https://github.com/armadaproject/armada/blob/master/internal/server/permissions/permissions.go).

### Queue-specific permissions

The following queue-specific permission verbs control what actions can be taken per individual queues:

* `submit`
* `cancel`
* `reprioritize`
* `watch`

[Learn more about permission verbs](https://github.com/armadaproject/armada/blob/master/pkg/client/queue/permission_verb.go).

### User permissions

The following table shows which permissions are required for a user to access each API endpoint (either directly or via a group).
Note that queue-specific permission require a user to be bound to a global permission as well (shown as tuples in the table).

| Endpoint           | Global Permissions      | Queue Permissions |
|--------------------|-------------------------|-------------------|
| `SubmitJobs`       | `submit_any_jobs`       | `submit`          |
| `CancelJobs`       | `cancel_any_jobs`       | `cancel`          |
| `ReprioritizeJobs` | `reprioritize_any_jobs` | `reprioritize`    |
| `CreateQueue`      | `create_queue`          |                   |
| `UpdateQueue`      | `create_queue`          |                   |
| `DeleteQueue`      | `delete_queue`          |                   |
| `GetQueue`         |                         |                   |
| `GetQueueInfo`     | `watch_all_events`      | `watch`           |
| `GetJobSetEvents`  | `watch_all_events`      | `watch`           |
