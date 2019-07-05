# K8S Batch
Experimental application to submit and monitor jobs using kubernetes cluster(s), providing Condor-like behaviour.

## Why?
In our Condor clusters we need to handle large spikes of resource requests. Condor queues thousands of jobs per user and slowly works them all off assuring all users get a fair share of resource.
Kubernetes itself is not designed around this use case and multiple components of the system struggle when 10k - 100k pods are created at once.
Some of the issues could be solved by replacing the scheduler or improving other components, but we also need to support large clusters and current Kubernetes official limit for nodes is 5000. We have anecdotal evidence from conferences that Kubernetes does not operate optimally past 1000 nodes without significant tuning.
It would be a benefit to have a solution that supports scaling out using multiple Kubernetes clusters. This allows simple scaling as well as benefit from a maintenance perspective.

## Overview
This application stores queues for users/projects with pod specifications and create these pods once there is available resource in Kubernetes.
To achieve fairness between users we have implemented a Condor like algorithm to divide resources. Each queue has a priority. When pods from a queue use some resources over time, queue priority is reduced so other queues will get more share in the future. When queues do not use resources their priority will eventually get back to initial value.

Current implementation utilises Redis to store queues of jobs. Redis streams are used for job events.

![Diagram](./batch-api.svg)



## Developer setup
Run local redis 
```
sudo docker run --expose=6379 --network=host redis
```
