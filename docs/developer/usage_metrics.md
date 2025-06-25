# Getting usage metrics

You can use the executor to report how much CPU or memory your jobs are using.

To turn on reporting, add the following to your executor config file:

``` yaml
metric:
   exposeQueueUsageMetrics: true
```

Metrics are calculated by getting values from `metrics-server`.

To develop locally with Kind, you need to deploy `metrics-server`. You can do this by applying the following to your Kind cluster:

```
kubectl apply -f https://gist.githubusercontent.com/hjacobs/69b6844ba8442fcbc2007da316499eb4/raw/5b8678ac5e11d6be45aa98ca40d17da70dcb974f/kind-metrics-server.yaml
```
