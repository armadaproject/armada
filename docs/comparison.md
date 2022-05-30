# Comparison of Kubernetes batch systems

## kube-batch

https://github.com/kubernetes-sigs/kube-batch

- k8s native batch scheduler.
- Supports preemption, gang scheduling, leader election for high availability, backfilling.
- Works on the k8s job type.
- Does not support:
  - Data management
  - Accelerator (Kubelet), e.g., GPUs
  - Isolation for multi-tenant
  - Job management
  - Other container runtimes
- Used by
  - Kubeflow
  - Volcano
- Looks to support many of the things we need

## Volcano

https://github.com/volcano-sh/volcano

- Volcano is a batch system built on Kubernetes.
- Volcano provides a suite of mechanisms required by many classes of batch and elastic workloads.
- Volcano integrates with compute frameworks, e.g., TensorFlow, Spark, PyTorch, and MPI.
- Has a batch scheduler based on kube-batch.
- There's a spark-on-k8s operator that supports Volcano integration
  - https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/volcano-integration.md
  - Using this operator, Spark jobs are submitted as a CRD with the "batchScheduler" field set to "volcano".
  - Then, Volcano will handle scheduling the Spark nodes in a way that is (somehow) more efficient.
- Volcano uses a CRD called PodGroup to group pods for batch scheduling
  -  https://volcano.sh/en/docs/podgroup/

## Kubeflow

https://www.kubeflow.org/

- A platform for developing and deplying a ML system.
- Kubeflow is a platform for data scientists who want to build and experiment with ML pipelines.
- Can spawn Jupyter notebooks backed by k8s nodes
- Uses CRDs for, e.g., TensorFlow jobs (TFJob).
- TFJob is designed to manage distributed TensorFlow training jobs.

## Horovod

https://github.com/horovod/horovod
https://towardsdatascience.com/distributed-deep-learning-training-with-horovod-on-kubernetes-6b28ac1d6b5d

- Horovod is a distributed deep learning training framework for TensorFlor, Keras, PyTorch, and Apache MXNet.
- The primary goal of Horovod is to simplify scaling a single-GPU ML training script to multiple GPUs.
- Claims that the MPI model is more straightforward than other solutions based on a parameter server, e.g., Distributed TensorFlow. I think they say this because they consider the parameter server model to be a special case of the MPI model. And that they consider the MPI model to the parameter server model + collective operations (e.g., allreduce).
- Horovod jobs can be run on Kubernetes by using, e.g., MPI Operator to setup/handle the MPI.

## MPI Operator

https://github.com/kubeflow/mpi-operator
https://medium.com/kubeflow/introduction-to-kubeflow-mpi-operator-and-industry-adoption-296d5f2e6edc

- Part of, and a core component of, Kubeflow.

## Armada

- There are many k8s ML frameworks, e.g., kube-batch, Volcano, Kubeflow.
- Each instance of these frameworks live in a single k8s cluster and support different workflows.
- There needs to be fair resource allocation between users that may be using different frameworks and clusters.
- For that reason, there needs to be a system that sits between the user and the framework they wish to use that decides where and in what order jobs should be run.
- Armada is a system that handles queuing and assigning jobs to ML frameworks across multiple k8s clusters.


- Batch scheduling and ML on k8s is a jungle.
- Lots of frameworks that do the same thing.
- It makes more and more sense to let Armada be a system for queuing a range of different resources to be submitted to underlying k8s clusters.
- So, for example, we would introduce gang scheduling by deploying kube-batch or volcano and adding a resource definition that kube-batch/volcano can process. So we don't build a proper scheduler. Instead, we use schedulers written by others.
- It'll get complex to get all of these things to play together. But not impossible.
- I've thought about this enough for now. I think what I'm saying makes sense. I don't think we should focus on this until maybe second half of 2022. We should first clean up the current Armada implementation.
- That requires cleaning up the architecture a bit. I have some thoughts.
- For today though, let's do a writeup on error handling.
- Let's write a small gRPC service that showcases the solution I want.