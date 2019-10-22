# Armada

![Armada](./logo.svg)

Armada is experimental application to submit and monitor jobs to multiple Kubernetes clusters.

It stores queues for users/projects with pod specifications and creates these pods once there is available resource in one of the connected Kubernetes clusters.

## Documentation

- [Design Documentation](./docs/design.md)
- [Development Guide](./docs/developer.md)
- [User Guide](./docs/user.md)
- [Installation in Production](./docs/production-install.md)

## Key features
- Armada maintains fair resource share over time (inspired by HTCondor priority)
- It can handle large amount of queued jobs (million+)
- It Allows adding and removing clusters from the system without disruption
- By utilizing multiple Kubernetes clusters system can scale to larger amount of nodes beyond one cluster limits

## Key concepts

**Queue:** Represent user or project, used to maintain fair share over time, has priority factor

**Job:** Unit of work to be run (described as Kubernetes PodSpec)

**Job Set:** Group of related jobs, api allows observing progress of job set together


## Try it out locally

Assuming you have go installed.

1. Clone repository & Build (project requires go & docker installed)
```bash
git clone https://github.com/G-Research/armada.git
cd armada
make build
```

2. Get kind
```bash
go get sigs.k8s.io/kind
```
 
3. create 2 kind clusters
```bash
kind create cluster --name demoA --config ./example/kind-config.yaml
kind create cluster --name demoB --config ./example/kind-config.yaml 
```

4. Start Redis
```bash
docker run -d --expose=6379 --network=host redis
```

5. Start server in one terminal
```bash
./bin/armada
```

6. Start executors for each cluster each in separate terminal
```bash
KUBECONFIG=$(kind get kubeconfig-path --name="demoA") ARMADA_APPLICATION_CLUSTERID=demoA ./bin/executor
KUBECONFIG=$(kind get kubeconfig-path --name="demoB") ARMADA_APPLICATION_CLUSTERID=demoB ./bin/executor
```
7. Create queue & Submit job
```bash
./bin/armadactl create-queue test 1
./bin/armadactl submit ./example/jobs.yaml
./bin/armadactl watch job-set-1
```
