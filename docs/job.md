# Armada Job

This page will cover what fields available in the Job spec file you submit with `armadactl submit`.

It has the same structure as the API so will also be reflected in any code clients you use.

Below is an example file using all available config:

```yaml
queue: example                            (1)
jobSetId: test                            (2)
jobs:
  - priority: 1000                        (3)
    namespace: example                    (4)
    clientId: 12345                       (5)
    labels:                               (6)
      example-label: "value"
    annotations:                          (7)
      example-annotation: "value"
    ingress:                              (8)
      - type: NodePort
        ports:
          - 5050
    podSpecs:                             (9)
      - containers:
        name: app
        imagePullPolicy: IfNotPresent
        image: vad1mo/hello-world-rest:latest
        securityContext:
          runAsUser: 1000
        resources:
          limits:
            memory: 1Gi
            cpu: 1
          requests:
            memory: 1Gi
            cpu: 1
        ports:
          - containerPort: 5050
            protocol: TCP
            name: http
```

Fields:
 - (1) The queue this job will be submitted to
 - (2) The name of the jobset this job will be submitted to
 - (3) The relative priority of the job being submitted. This priority is relative to the other jobs in the same `queue`
   - Jobs with a lower priority will be run before jobs with a higher priority. So to create a job at the front of your queue, set the priority to a lower number than any other job in the Queue.
 - (4) The namespace the pods created as part of this job will be submitted to
   - If not specified, the `default` namespace is used
 - (5) This is the id of the Job on the client. 
    - This is to prevent creating multiple Armada Jobs if the Job is submitted twice by the client, useful in case of network failures
    - Each submission will check if a Job with that `clientId` exists and return if it so. Otherwise a new Job will be created
    - To always create a new job, don't specify this field
 - (6) These labels will be added to all pods created as part of this Job
 - (7) These annotations will be added to all pods created as part of this Job
 - (8) A list of ports that will be exposed with the specified type
    - The ingress will only exopse ports for pods that also expose the corresponding port via containerPort
 - (9) A list of podSpecs that will determine the pods being created as part of the Job.
 
 