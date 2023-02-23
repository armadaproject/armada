Armada is an open-source tool that allows users to deploy and manage a collection of Kubernetes manifests as a single unit. AKS (Azure Kubernetes Service) is a managed Kubernetes service provided by Microsoft Azure. This guide will walk you through the steps required to deploy Armada to AKS.

### Before you begin, make sure you have the following:

An Azure subscription
Azure CLI installed and configured
Helm installed
Git installed
## Step 1: Clone Armada Repository

The first step is to clone the Armada repository to your local machine. You can do this by running the following command:

#### bash

git clone https://github.com/armadaplatform/armada.git

## Step 2: Create an AKS Cluster

Next, create an AKS cluster using the Azure CLI. You can use the following command to create a new AKS cluster:

#### css

az aks create --resource-group <resource-group-name> --name <cluster-name> --node-count <node-count> --enable-addons monitoring --generate-ssh-keys
Replace <resource-group-name> with the name of the resource group where you want to create the cluster, <cluster-name> with the name you want to give your AKS cluster, and <node-count> with the number of nodes you want to create.

## Step 3: Install Tiller

Tiller is the server-side component of Helm that manages the installation of charts into your Kubernetes cluster. You need to install Tiller on your AKS cluster before you can deploy Armada.

#### ruby

kubectl apply -f https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get
csharp
Copy code
helm init --service-account tiller

## Step 4: Deploy Armada

Now that Tiller is installed, you can deploy Armada to your AKS cluster. First, change your current directory to the armada directory you cloned in Step 1.

#### bash

cd armada
Next, use the following command to install Armada:

#### scss

helm install ./charts/armada --name armada --namespace armada --set armada.image.tag=latest --set armada.image.pullPolicy=Always
This command installs the armada chart from the armada directory, sets the image tag to latest, and pulls the image on every deployment.

## Step 5: Verify Armada Installation

To verify that Armada has been installed correctly, use the following command to check the status of the Armada pods:

#### csharp

kubectl get pods -n armada
You should see the Armada pods in a running state.

## Step 6: Deploy Manifests with Armada

Now that Armada is installed and running, you can use it to deploy your Kubernetes manifests as a single unit. To do this, create a new directory and place your manifests in it.

Next, create an armada.yaml file in the same directory with the following contents:

#### yaml

apiVersion: armada.airshipit.org/v1alpha1
kind: Armada
metadata:
name: my-manifests
spec:
target_manifest: my-manifests/*
Replace my-manifests with the name of the directory where your manifests are located.

#### Finally, use the following command to deploy your manifests using Armada:


kubectl apply -f armada.yaml
This command creates an Armada resource named my-manifests and specifies the directory where your manifests are located.

Congratulations! You have successfully deployed Armada to AKS and used it to deploy your Kubernetes manifests.