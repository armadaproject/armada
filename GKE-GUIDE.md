### Armada is an open-source tool for deploying and managing containerized applications across multiple Kubernetes clusters. GKE (Google Kubernetes Engine) is a managed Kubernetes service provided by Google Cloud. In this guide, we will walk through the steps to deploy Armada to GKE.

# Prerequisites
Before we begin, make sure you have the following prerequisites:

A GCP (Google Cloud Platform) account with billing enabled
GCP SDK (Software Development Kit) installed on your local machine
kubectl command-line tool installed on your local machine
## Step 1: Create a GKE cluster
First, we need to create a GKE cluster where we will deploy Armada. Follow these steps to create a GKE cluster:

Open the GCP console and select the project where you want to create the GKE cluster.
In the left sidebar, click on "Kubernetes Engine" and then click on "Clusters".
Click on the "Create cluster" button.
In the "Cluster basics" section, enter a name for your cluster and choose the desired region and zone.
In the "Node pools" section, choose the machine type and the number of nodes for your cluster.
Leave the default values for the remaining settings and click on the "Create" button.
It may take a few minutes for the GKE cluster to be created.

## Step 2: Set up kubectl
Next, we need to set up kubectl to connect to the GKE cluster we just created. Follow these steps to configure kubectl:

Open the Cloud Shell in the GCP console or open a terminal on your local machine.
Run the following command to authenticate kubectl with your GCP account:

#### gcloud auth login
Run the following command to configure kubectl to use the GKE cluster:

#### gcloud container clusters get-credentials [CLUSTER_NAME]
Replace [CLUSTER_NAME] with the name of your GKE cluster.

Verify that kubectl is configured correctly by running the following command:

#### kubectl get nodes
You should see a list of nodes in your GKE cluster.

## Step 3: Deploy Armada to GKE
Now that we have a GKE cluster and kubectl is set up, we can deploy Armada to the cluster. Follow these steps to deploy Armada:

Clone the Armada GitHub repository:

#### git clone https://github.com/armadaplatform/armada.git
Change into the armada/deploy/gke directory:


#### cd armada/deploy/gke
Edit the values.yaml file to configure the Armada deployment. You can change the values of armada.image.tag, armada.ingress.host and armada.ingress.enabled based on your requirements.

Deploy Armada to the GKE cluster using the following command:


#### helm install armada . -f values.yaml
This will deploy Armada to the default namespace in your GKE cluster.

Verify that Armada is running by running the following command:


kubectl get pods
You should see the Armada pods running.

## Step 4: Access the Armada UI
Finally, we can access the Armada UI to manage our Kubernetes clusters. Follow these steps to access the Armada UI:

Get the external IP address of the Armada ingress:


#### kubectl get ingress armada -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
Open a web browser and enter the external IP address obtained in step 1.





## To achieve the goal of having reproducible Terraform scripts for creating the GKE cluster and setting up Armada, we need to follow these steps:

### Step 1: Set up Terraform
To set up Terraform, follow these steps:

Download and install Terraform on your local machine: https://www.terraform.io/downloads.html

Verify the installation by running the following command:


#### terraform --version
Create a new directory where we will store our Terraform scripts:

#### mkdir armada-gke-terraform
#### cd armada-gke-terraform
### Step 2: Create the Terraform scripts
Next, we need to create the Terraform scripts for creating the GKE cluster and setting up Armada. Here are the steps to follow:

1. Create a new file named main.tf in the armada-gke-terraform directory.

2. Add the following code to the main.tf file to create a GKE cluster:


provider "google" {
project = "<PROJECT_ID>"
region  = "<REGION>"
}

resource "google_container_cluster" "cluster" {
name               = "armada-cluster"
location           = "${var.region}-a"
initial_node_count = 3

node_config {
machine_type = "n1-standard-2"

    metadata = {
      disable-legacy-endpoints = "true"
    }

    oauth_scopes = [
      "https://www.googleapis.com/auth/compute",
      "https://www.googleapis.com/auth/devstorage.read_only",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring",
      "https://www.googleapis.com/auth/service.management.readonly",
      "https://www.googleapis.com/auth/servicecontrol",
      "https://www.googleapis.com/auth/trace.append"
    ]
}

master_auth {
username = ""
password = ""

    client_certificate_config {
      issue_client_certificate = false
    }
}

lifecycle {
ignore_changes = [node_config[0].metadata]
}
}

output "cluster_endpoint" {
value = "${google_container_cluster.cluster.endpoint}"
}

output "cluster_ca_certificate" {
value = "${google_container_cluster.cluster.master_auth.0.cluster_ca_certificate}"
}
Replace <PROJECT_ID> and <REGION> with your GCP project ID and preferred region.

3. Add the following code to the main.tf file to set up Armada:

resource "kubernetes_namespace" "armada" {
metadata {
name = "armada"
}
}

resource "helm_release" "armada" {

name      = "armada"

namespace = kubernetes_namespace.armada.metadata.0.name

repository {

name = "armada"

url  = "https://armada-charts.storage.googleapis.com"
}

chart {

name    = "armada"

version = "1.0.0"
}
values = [
<<EOT
armada:
ingress:
enabled: true
annotations:
kubernetes.io/ingress.class: "nginx"
image:
repository: "gcr.io/armada-charts/armada"
tag: "1.0.0"
replicaCount: 1
resources:
limits:
cpu: "500m"
memory: "512Mi"
requests:
cpu: "250m"
memory: "256Mi"
ingress:
host: "armada.example.com"
EOT
]
}
This code will create a new namespace named armada and deploy






