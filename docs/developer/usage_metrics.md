# Getting usage metrics

You can use the executor to report how much CPU or memory your jobs are using.

To turn on reporting, add the following to your executor config file:

``` yaml
metric:
   exposeQueueUsageMetrics: true
```

Metrics are calculated by getting values from [`metrics-server`](https://github.com/kubernetes-sigs/metrics-server), a Kubernetes extension for retrieving and calculating metrics on Kubernetes cluster members.

## Developing locally with kind

kind is a small implementation of Kubernetes, packaged as a single executable program. You can use it to get a base Kubernetes cluster running on your local system, enabling you to deploy, develop and test Armada components. [Learn more about kind](https://kind.sigs.k8s.io/).

To develop locally with kind, you first need to deploy `metrics-server`. You can do this by applying the following to your kind cluster:

```
kubectl apply -f https://gist.githubusercontent.com/hjacobs/69b6844ba8442fcbc2007da316499eb4/raw/5b8678ac5e11d6be45aa98ca40d17da70dcb974f/kind-metrics-server.yaml
```
