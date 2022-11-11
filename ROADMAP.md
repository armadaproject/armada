# Roadmap

## Guided User Journeys through cloud vendor kubernetes platforms

### Motivation

At KubeCon, there was a strong interest in Armada and how it can work with cloud vendors.  We should consider deployment guides for various cloud vendors.  The big three are Amazon (EKS), Google (GKE), and Azure (AKS).  RedHat seemed to play a big role in the open source side so we maybe should consider openshift also.

### Work Needed

a) Move our EKS deployment repo to Armada

b) Create a guide for Azure (AKS) [Issue](https://github.com/G-Research/armada/issues/1760)

c) Create a guide for Google (GKE) [Issue](https://github.com/G-Research/armada/issues/1759)

d) Create a guide for Openshift [Issue](https://github.com/G-Research/armada/issues/1780)

## Queue Conversion to use CRD

### Motivation

A major goal is to make deployment/support of Armada much easier.  Currently, our queue object is a custom object defined in our submit.proto file.  We are missing key lifecycle methods.  Common operations such as editing or getting a list of created queues are missing from our API.  We implement this API separately and store it in our internal data store for Armada.  This epic should aim to start storing these objects in the api-server in Kubernetes.

This epic will include designing the new queue api and storing it as part of Kubernetes.  This should allow us to support creation/managing of queue objects with standard kubernetes tooling rather than having to implement these features ourselves.

## Adopt the Job Controller for running Armada Jobs

### Motivation

A bad practice for running Kubernetes workloads is to schedule naked pods on your cluster.  Kubernetes is designed to work with higher level controllers that verify the contract of your application.  Think of a deployment object with x amount of replicas.  Kubernetes will always make sure your object has x amount of replicas so if a node goes down, kubernetes will restart those objects.

This pattern can be extended to batch jobs by using the job controller.  There is some momentum around the Job API in native kubernetes.  The job controller does provide retrying ability and should allow scheduling of pods on healthy nodes.  This also allows us to use Job features that are being released to better support batch workloads.