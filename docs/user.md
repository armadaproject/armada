# User Guide

## How to use Armada

The main concepts to understand in Armada are:
* Job
* Job Set
* Queue

### Job

A Job in Armada is equivalent to a Pod in Kubernetes, with a few extra metadata fields.
 
It is the most basic unit of work in Armada and if you are familiar with Kubernetes it will be very familiar to you.
  
A simple example of a job in yaml form looks like:

```yaml
queue: test
priority: 0
jobSetId: set1
podSpec:
  terminationGracePeriodSeconds: 0
  restartPolicy: Never
  containers:
    - name: sleep
      imagePullPolicy: IfNotPresent
      image: busybox:latest
      args:
        - sleep
        - 60s
      resources:
        limits:
          memory: 64Mi
          cpu: 150m
        requests:
          memory: 64Mi
          cpu: 150m
```
A Job belongs to a Queue and a Job Set, seen by the corresponding fields. Finally it has a priority, which is a relative priority to other Jobs in the same queue; the lower the number, the higher the priority.

If you submit 2 Jobs, A and B, and you want B to be ordered before A in the queue, B should be given a priority lower than A using the priority field.

All jobs of priority 0 will be taken from the queue before any with priority 1 and time of submission is not taken into account.

**Note: Job resource request and limit should be equal. Armada does not support limit > request currently.**

### Job Set

A Job Set is a logical grouping of Jobs.

The reasons you may want to do this is because you can;
* Watch all events of a Job Set together
* Cancel all jobs in a Job Set together

The Job Sets are typically meant to be used to represent a single task, where many Jobs are involved.

You can then follow this Job Set as a single entity rather than having to track multiple associated Jobs.

A Job Set has no impact on the running of jobs a this moment and is purely an abstraction over a group of Jobs.

### Queue

A queue is the likely most important aspect of Armada.

Conceptually is it quite simple, it is just a Queue of jobs waiting to be run on a Kubernetes cluster.

In addition to being a queue, it is also used:
* Maintaining fair share over time
* Security boundary (Not yet implemented) between jobs

##### Fair share

Queues are provided with a fair share of the resource available on all clusters over time. 

That is to say if there are 5 queues and they were all to submit infinite jobs (well in excess of cluster capacity), then each queue should receive the  same amount of compute resource over the course of time. No single queue should get a larger share of the available resource.

The reason we say over time, as at any single moment we cannot guarantee the queues will be exactly equal, however each job submitted has a cost. So if one queue gets a larger share early, they'll receive a lesser share later to make up for it.

##### Priority Factor

A Queue has a Priority Factor.

The lower the number, the greater the share that queue will receive.

So if you have 2 Queues:
* A - Priority 10
* B - Priority 1

We would expect B to receive substantially more resource on the clusters than A over time. 

All priorities are relative, so you can make your own scheme of priority numbers. So for example you could have "normal" be 100, "high" be 50 and "max" be 10.

Priority allows great flexibility, as it means you can predictably give certain queues a bigger share of resource than others.

##### Security Boundary

Armada allows to set user (and group) permissions for a specific Queue using owners (and groupOwners) options. 

The job set events are available to all users with "watch_all_events" permissions.

If `kubernetes.impersonateUsers` is turned on, Armada will create pods in kubernetes impersonating owner of the job. This will enforce Kubernetes permissions and limit access to namespaces.

#### Considerations when setting up Queues

So now you know what Queues are and what they can do. We'll briefly cover what to consider when setting them up.

You should consider how your organisation should be split up so users' work is provided with an appropriate amount of resource allocation.

There are many ways this could be done, here are some ideas:

* By user. All users will be equal (or scaled) relative to each other
* By team. All teams will be equal (or scaled) relative to each other
* By project. All projects will be equal (or scaled) relative to each other
* A mixture of all the above. So a given user may have their own queue, be part of a team queue and part of multiple project queues. They would then submit the appropriate jobs to the appropriate queues and expect them to all be given a fair amount of resource.
    * This allows a lot of flexibility in how you slice up your organisation, and combined with differing priorities it is possible to achieve any mix of resource allocation you want.

For each of the above, if you went for "By user". Simply make each user of the system their own Queue and tell them to submit to it. 

They will then receive the same amount of resource as any other user in the system.
