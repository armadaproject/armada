# EC2 Developer Setup

## Background

For development, you might want to set up an Amazon EC2 instance as the resource requirements for Armada are substantial. A typical Armada installation requires a system with at least 16GB of memory to perform well. Running Armada on a laptop made before ~2017 will typically eat battery life and result in a slower UI.

Note: As of June 2022, not all Armada dependencies reliably build on a Mac M1 using standard package management. So if you have an M1 Mac, working on EC2 or another external server is your best bet.

## Instructions

- We suggest a t3.xlarge instance from aws ec2 with AmazonLinux as the OS.  16 GB of memory is suggested.
- During selection of instance, Add a large volume to your ec2 instance.   100 gb of storage is recommended.
- When selecting the instance, you will have the opportunity to choose a security group. You may need to make a new one. Be sure to add a rule allowing inbound communication on port 22 so that you can access your server via SSH. We recommend that you restrict access to the IP address from which you access the Internet, or a small CIDR block containing it.

If you want to use your browser to access Armada Lookout UI or other web-based interfaces, you will also need to grant access to their respective ports. For added security, consider using an [SSH tunnel](https://www.ssh.com/academy/ssh/tunneling/example) from your local machine to your development server instead of opening those ports. You can add LocalForward to your ssh config: `LocalForward 4000 localhost:3000`

- ### Install [Docker](https://www.cyberciti.biz/faq/how-to-install-docker-on-amazon-linux-2/)

The procedure to install Docker on AMI 2 (Amazon Linux 2) running on either EC2 or Lightsail instance is as follows:

<b>1. Login into remote AWS server using the ssh command:</b>

```
ssh ec2-user@ec2-ip-address-dns-name-here
```

<b>2. Apply pending updates using the yum command:</b>

```
sudo yum update
```

<b>3. Search for Docker package:</b>

```
sudo yum search docker
```

<b>4. Get version information:</b>

```
sudo yum info docker
```
<p align = "center">
<img src = "https://user-images.githubusercontent.com/101946115/224549681-f8d6d08c-6dae-45e4-a180-13d59a1a3dfb.png" height = 500 width = 400/>
</p>

<b>5. Install docker, run:</b>

```
sudo yum install docker
```

<b>6. Add group membership for the default ec2-user so you can run all docker commands without using the sudo command:</b>

```
sudo usermod -a -G docker ec2-user
id ec2-user
# Reload a Linux user's group assignments to docker w/o logout
newgrp docker
```


- ### Install [docker-compose](https://www.cyberciti.biz/faq/how-to-install-docker-on-amazon-linux-2/)

```bash
$ cd $HOME/.docker
$ mkdir cli-plugins
$ cd cli-plugins
$ curl -SL https://github.com/docker/compose/releases/download/v2.17.3/docker-compose-linux-x86_64 -o docker-compose
$ chmod 755 docker-compose
```

Then verify it with:

```bash
docker-compose version
```

- ### Getting the [Docker Compose Plugin](https://docs.docker.com/compose/install/linux/#install-the-plugin-manually)

Armadas setup assumes You have the docker compose plugin installed.  If you do not have it installed, you can use the following guide:

* https://docs.docker.com/compose/install/linux/#install-the-plugin-manually

Then test it with:

```bash
docker compose version
```


- ### Install [Go](https://go.dev/doc/install)

ssh into your EC2 instance, become root and download the go package from [golang.org](https://go.dev/doc/install).

<b>1. Extract the archive you downloaded into /usr/local, creating a Go tree in /usr/local/go with the following command:</b>

```
rm -rf /usr/local/go && tar -C /usr/local -xzf go1.20.1.linux-amd64.tar.gz
```

<b>2. Configure .bashrc</b>

Switch back to ec2-user and add the following line to your ~/.bashrc file

```
export PATH=$PATH:/usr/local/go/bin
```

<b>3. Configure go Environment</b>

Add the following lines to your ~/.bashrc file as well, also create a golang folder under /home/ec2-user.

```
# Go envs
export GOVERSION=go1.20.1
export GO_INSTALL_DIR=/usr/local/go
export GOROOT=$GO_INSTALL_DIR
export GOPATH=/home/ec2-user/golang
export PATH=$GOROOT/bin:$GOPATH/bin:$PATH
export GO111MODULE="on"
export GOSUMDB=off
```

<b>4. Test go</b>

Verify that you’ve installed Go by opening a command prompt and typing the following command:

```
go version
go version go1.20.1 linux/amd64
```

- ### Install [Kind](https://dev.to/rajitpaul_savesoil/setup-kind-kubernetes-in-docker-on-linux-3kbd)

<b>1. Install Kind</b>

```
go install sigs.k8s.io/kind@v0.11.1
# You can replace v0.11.1 with the latest stable kind version
```

<b>2. Move the KinD Binary to /usr/local/bin</b>

```
- You can find the kind binary inside the directory go/bin
- Move it to /usr/local/bin - mv go/bin/kind /usr/local/bin
- Make sure you have a path setup for /usr/local/bin
```

- ### Install [kubectl](https://dev.to/rajitpaul_savesoil/setup-kind-kubernetes-in-docker-on-linux-3kbd)

<b>1. Install Latest Version of Kubectl:</b>

```
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl
mv kubectl /usr/local/bin
```

- ### Install [helm](https://helm.sh/docs/intro/install/)

<b>1. Install helm:</b>

```
curl -sSL https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash
```

<b>2. We can verify the version</b>

```
helm version --short
```

- ### Install [python3 (>= 3.7)](https://www.geeksforgeeks.org/how-to-install-python3-on-aws-ec2/)

<b>1. Check if Python is already installed or not on our AWS EC2.</b>

```
python --version
```

<p align = "center">
<img src = "https://user-images.githubusercontent.com/101946115/224551503-ab9b39b6-802c-432f-ae4c-fcc0adbfd00b.png" />
</p>

<b>2. At first update, Ubuntu packages by using the following command.</b>

```
sudo apt update
```

<p align = "center">
<img src = "https://user-images.githubusercontent.com/101946115/224551609-5f38e650-a714-4cc6-b68c-93c17c0fd296.png" />
</p>

<b>3. If Python3 is not installed on your AWS EC2, then install Python3 using the following command.</b>

```
sudo apt-get install python3.7
```

<b>4. We have successfully installed Python3 on AWS EC2, to check if Python3 is successfully installed or not, verify using the following command.</b>

```
python3 --version
```

- ### Install .NET for [linux](https://docs.microsoft.com/en-us/dotnet/core/install/linux-centos)

<b>1. Before you install .NET, run the following commands to add the Microsoft package signing key to your list of trusted keys and add the Microsoft package repository. Open a terminal and run the following commands:</b>

```
sudo rpm -Uvh https://packages.microsoft.com/config/centos/7/packages-microsoft-prod.rpm
```

<b>2. Install the SDK</b>

```
sudo yum install dotnet-sdk-7.0
```

<b>3. Install the runtime</b>

```
sudo yum install aspnetcore-runtime-7.0
```

- ### We suggest using the [remote code extension](https://code.visualstudio.com/docs/remote/ssh) for VS Code if that is your IDE of choice.

<p align = "center">
<img src = "https://user-images.githubusercontent.com/101946115/224552173-6a4e15f8-1db0-453f-9a9f-683d2c53c2a1.png" />
</p>

- ### Please see [Our Developer Docs](../developer.md) for more information on how to get started with the codebase.
