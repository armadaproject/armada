# Inspect and Debugging etcd in Localdev setup

When developing or testing Armada in the Localdev setup, it's sometimes helpful
to directly query the etcd database to gather various statistics. However, by
default, the `kind` tool (for running Kubernetes clusters inside a local Docker
side) does not expose the etcd interface for direct querying. The following
steps give a solution for querying an etcd instance running inside of `kind`.


First, verify the running nodes and podes.
```bash
$ kubectl get nodes -A
NAME                        STATUS   ROLES           AGE   VERSION
armada-test-control-plane   Ready    control-plane   78m   v1.24.7
armada-test-worker          Ready    <none>          77m   v1.24.7

$ kubectl get pods -A
NAMESPACE            NAME                                                READY   STATUS      RESTARTS   AGE
ingress-nginx        ingress-nginx-admission-create-9xnpn                0/1     Completed   0          78m
ingress-nginx        ingress-nginx-admission-patch-phkgm                 0/1     Completed   1          78m
ingress-nginx        ingress-nginx-controller-646df5f698-zbgqz           1/1     Running     0          78m
kube-system          coredns-6d4b75cb6d-9z87w                            1/1     Running     0          79m
kube-system          coredns-6d4b75cb6d-flz4r                            1/1     Running     0          79m
kube-system          etcd-armada-test-control-plane                      1/1     Running     0          79m
kube-system          kindnet-nx952                                       1/1     Running     0          79m
kube-system          kindnet-rtqkc                                       1/1     Running     0          79m
kube-system          kube-apiserver-armada-test-control-plane            1/1     Running     0          79m
kube-system          kube-controller-manager-armada-test-control-plane   1/1     Running     0          79m
kube-system          kube-proxy-cwl2r                                    1/1     Running     0          79m
kube-system          kube-proxy-wjqft                                    1/1     Running     0          79m
kube-system          kube-scheduler-armada-test-control-plane            1/1     Running     0          79m
local-path-storage   local-path-provisioner-6b84c5c67f-22m8m             1/1     Running     0          79m
```
You should see an etcd control plane pod in the list of pods.

Copy the etcdclient deployment YAML into the cluster control plane node:

```bash
$ docker cp developer/config/etcdclient.yaml armada-test-control-plane:/
```

Then, open a shell in the control plane node:
```bash
$ docker exec -it -u 0 --privileged armada-test-control-plane  /bin/bash
```

In the container shell, move the deployment YAML file to the Kubernetes deployments source
directory. Kubernetes (Kind) will notice the file's appearance and will deploy
the new pod.
```bash
root@armada-test-control-plane:/# mv etcdclient.yaml /etc/kubernetes/manifests/
root@armada-test-control-plane:/# exit
$ kubectl get pods -A
```
You should see an etcdclient pod running.

Open a shell in the new etcdclient utility pod, and start using `etcdctl` to query etcd.
```bash
$ kubectl exec -n kube-system -it etcdclient-armada-test-control-plane -- sh
/ # etcdctl endpoint status -w table
+-------------------------+------------------+---------+---------+-----------+-----------+------------+
|        ENDPOINT         |        ID        | VERSION | DB SIZE | IS LEADER | RAFT TERM | RAFT INDEX |
+-------------------------+------------------+---------+---------+-----------+-----------+------------+
| https://172.19.0.2:2379 | d7380397c3ec4b90 |   3.5.3 |  3.9 MB |      true |         2 |      16727 |
+-------------------------+------------------+---------+---------+-----------+-----------+------------+
/ #
/ # exit
```
At this point, you can use `etcdctl` to query etcd for key-value pairs, get the health and/or metrics
of the etcd server.

## References

https://mauilion.dev/posts/etcdclient/
