# Kubernetes native authentication
- [Kubernetes native authentication](#kubernetes-native-authentication)
  - [Authentication flow](#authentication-flow)
  - [Setup](#setup)
    - [Kid-mapping ConfigMap](#kid-mapping-configmap)
    - [Server configuration](#server-configuration)
      - [Example config](#example-config)
    - [Client configuration](#client-configuration)
      - [Example config](#example-config-1)

Kubernetes native authentication defines a flow where the Armada Executor cluster's in-built token handling authenticates the Executor requesting jobs from the Armada Server.

## Authentication flow

1. The Executor requests a temporary Service Account Token from its own cluster's 
Kubernetes API using TokenRequest.
2. The Executor sends this Token to the Server in an Authorization Header.
3. The Server decodes the token to read the KID (Kubernetes Key ID).
4. The Server swaps for the Callback URL of the executor's API server. The KID-to-URL mapping is stored in a prior generated ConfigMap or Secret.
5. The Server uses the URL to call the Executor Cluster's TokenReview endpoint.
6. On successful review, the Server caches the Token for its lifetime. On unsuccessful review, the Token is cached for a configuration defined time.

## Setup

### Kid-mapping ConfigMap

The Armada Server must have a ConfigMap in its namespace with the following format for entries:

```yaml
data:
  "<CLUSTER_KID>": "<EXECUTOR_CLUSTER_CALLBACK_URL>"
  ...
```

You can mount this ConfigMap anywhere on the Server's Pod.

### Server configuration

You need to configure three things in the Server Config:

* the location of the KID-mapping config map mounted on the Pod
* the retry timeout for failed Tokens
* the full service account name in the `permissionGroupMapping`

#### Example config

```yaml
applicationConfig:
  auth:
    kubernetesAuth:
      kidMappingFileLocation: "/kid-mapping/"
      invalidTokenExpiry: 60
      permissionGroupMapping:
        execute_jobs: ["system:serviceaccount:armada:armada-executor"]
```

### Client configuration

For the Executor authentication, you need to specify:

* the desired token `expiry`
* the Executor's `namespace` and `serviceAccount`

#### Example config

```yaml
applicationConfig:
  kubernetesNativeAuth:
    expiry: 3600
    namespace: "armada"
    serviceAccount: "armada-executor"
```