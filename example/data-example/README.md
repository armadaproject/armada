# Setting up quick start and use local file system

You can use the kind-data-config-executor.yaml config to setup a kind cluster that attaches a local file system.

Kind, by default, does not attach volumes from your local file system.  

```
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    image: "kindest/node:v1.21.10"
    extraMounts:
      - hostPath: /tmp/data
        containerPath: /data
  - role: worker
    image: "kindest/node:v1.21.10"
    extraMounts:
      - hostPath: /tmp/data
        containerPath: /data
```
The above config creates a kind executor cluster that mounts /tmp/data.

Once you have kind setup to use these volumes, you can now create kubernetes objects such as PersistentVolumes.

We have an example PersistentVolume and PersistentVolumeClaim for a local kind setup.  

A user can then use the PersistentVolumeClaim in their armada job spec:

```
queue: test
jobSetId: job-set-1
jobs:
  - priority: 0
    podSpec:
      terminationGracePeriodSeconds: 0
      restartPolicy: Never
      containers:
        - name: docker-data
          imagePullPolicy: IfNotPresent
          image: kevinpatrickhannon/docker-data:1.0.1
          command:
            - /app/go-file
          args:
            - --input=/data/test.csv
            - --output=/data/out.txt
          volumeMounts:
          - name: data
            mountPath: /data
          resources:
            limits:
              memory: 64Mi
              cpu: 150m
            requests:
              memory: 64Mi
              cpu: 150m
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: myclaim-1
```

### Setting up volumes with kind cluster

Kind is a simple way of testing kubernetes clusters with docker.

`kind get clusters` Will show you a list of kind clusters

`kubectl config get-contexts` will show the kubernetes contexts.

You can use kubectl and the context to interact with that particular kubernets cluster.

`kubectl create -f examples/data-example/persistent-volume.yaml --context $KUBE_CONTEXT`

This will create a Persistent-Volume in the kind cluster

`kubectl create -f examples/data-example/persistent-volume-claim.yaml --context $KUBE_CONTEXT`

