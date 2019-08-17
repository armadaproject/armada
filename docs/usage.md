# Installation
Armada uses redis database, you can either use AWS managed Redis or install redis to your Kubernetes cluster (https://github.com/helm/charts/tree/master/stable/redis-ha).

## Installing Armada Executor
TODO
```bash
helm install ./deployment/armada-executor
```
## Installing Armada Server
TODO
```bash
helm install ./deployment/armada
```

# Submitting Jobs
Describe jobs in yaml file:
```yaml
jobs:
  - queue: test
    priority: 0
    jobSetId: job-set-1
    podSpec:
      terminationGracePeriodSeconds: 0
      restartPolicy: Never
      containers:
          ... any Kubernetes pod spec ...

```

Use armadactl command line utility to submit jobs to armada server
```bash
# create a queue:
armadactl create-queue test 1

# submit jobs in yaml file:
armadactl submit ./example/jobs.yaml

# watch jobs events:
armadactl watch job-set-1

```
