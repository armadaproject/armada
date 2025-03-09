# Kubernetes Native Authentication

Kubernetes native authentication defines a flow where the Armada
Executor cluster's in-built token handling is used to authenticate
the Executor requesting jobs from the Armada Server.

## Authentication Flow

1. The Executor requests a temporary Service Account Token from its own cluster's 
Kubernetes API using TokenRequest
2. The Executor sends this Token to the Server in an Authorization Header.
3. The Server decodes the token to read the KID (Kubernetes Key ID).
4. The Server swaps for the Callback URL of the executor's API server.
The KID to URL mapping is stored in a prior generated ConfigMap or Secret.
5. The Server uses the URL to call the Executor Cluster's TokenReview endpoint.
6. On successful review the Server caches the Token for its lifetime,
on unsuccessful review the Token is cached for a configuration defined time.

## Setup

### Kid-mapping ConfigMap

The Armada Server must have a ConfigMap in its namespace with the following
format for entries:

```yaml
data:
  "<CLUSTER_KID>": "<EXECUTOR_CLUSTER_CALLBACK_URL>"
  ...
```

This ConfigMap may be mounted anywhere on the Server's Pod.

### Server configuration

Three things need to be configured in the Server Config:
- The location of the KID-mapping config map mounted on the Pod
- The retry timeout for failed Tokens
- The full service account name in the permissionGroupMapping

Example Config:
```yaml
applicationConfig:
  auth:
    kubernetesAuth:
      kidMappingFileLocation: "/kid-mapping/"
      invalidTokenExpiry: 60
      permissionGroupMapping:
        execute_jobs: ["system:serviceaccount:armada:armada-executor"]
```

### Client Configuration

For the Executor authentication you will need to specify:
- The desired token Expiry
- The Executor's Namespace and ServiceAccount

Example Config:
```yaml
applicationConfig:
  kubernetesNativeAuth:
    expiry: 3600
    namespace: "armada"
    serviceAccount: "armada-executor"
```