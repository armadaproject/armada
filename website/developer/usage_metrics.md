## Usage metrics

Some functionality the executor has is to report how much cpu/memory jobs are using.

This is turned on by changing the executor config file to include:
``` yaml
metric:
   exposeQueueUsageMetrics: true
```

The metrics are calculated by getting values from metrics-server.

When developing locally with Kind, you will also need to deploy metrics-server to allow this to work.

The simplest way to do this it to apply this to your kind cluster:

```
kubectl apply -f https://gist.githubusercontent.com/hjacobs/69b6844ba8442fcbc2007da316499eb4/raw/5b8678ac5e11d6be45aa98ca40d17da70dcb974f/kind-metrics-server.yaml
```
