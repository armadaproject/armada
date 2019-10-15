## Metrics

All Armada components provide /metrics endpoints providing relevant metrics to the running of the system.

We actively support Prometheus through our Helm charts (see below for details), however any metrics solution that can scrape an end point will work.

### Component metrics

#### Server

The server component provides metrics on the :9000/metrics endpoint.

You can enable Prometheus components when installing Helm with setting `prometheus.enabled=true`.

#### Executor

The server component provides metrics on the :9001/metrics endpoint.

You can enable Prometheus components when installing Helm with setting `prometheus.enabled=true`.
