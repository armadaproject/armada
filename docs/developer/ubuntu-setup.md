# Setting up an Ubuntu Linux instance for Armada development

## Introduction

This document is a list of the steps, packages, and tweaks that need to be done to get an Ubuntu Linux
instance running, with all the tools needed for Armada development and testing.

The packages and steps were verified on an AWS EC2 instance (type t3.xlarge, 4 vcpu, 16GB RAM,
150GB EBS disk), but should be essentially the same on any comparable hardware system.

### Install Ubuntu Linux

Install Ubuntu Linux 22.04 (later versions may work as well). The default package set should
work. If you are setting up a new AWS EC2 instance, the default Ubuntu 22.04 image works well.

When installing, ensure that the network configuration allows:
- SSH traffic from your client IP(s)
- HTTP traffic
- HTTPS traffic

Apply all recent updates:
```
$ sudo apt update
$ sudo apt upgrade
```
You will likely need to reboot after applying the updates:
```
$ sudo shutdown -r now
```
After logging in, clean up any old, unused packages:
```
$ sudo apt autoremove
```

AWS usually creates new EC2 instances with a very small root partion (8GB), which will quickly
fill up when using containers, or doing any serious development. Creating a new,  large EBS volume, and 
attaching it to the instance, will give a system usable for container work.

First, provision an EBS volume in the AWS Console - of at least 150GB, or more - and attach it to
the instance. You will need to create the EBS volume in the same availability zone as the EC2
instance - you can find the latter's AZ by clicking on the 'Networking' tab in the details page
for the instance, and you should see the Availabilty Zone listed in that panel. Once you've created
the volume, attach it to the instance.

Then, format a filesystem on the volume and mount it. First, determine what block device the
parition is on, by running the `lsblk` comand. There should be a line where the TYPE is 'disk'
and the size matches the size you specified when creating the volume - e.g.
```
nvme1n1      259:4    0   150G  0 disk
```
Create a filesystem on that device by running `mkfs`:
```
$ sudo mkfs -t ext4 /dev/nvme1n1
```
Then set a label on the partition - here, we will give it a label of 'VOL1':
```
$ sudo e2label /dev/nvme1n1 VOL1
```
Create the mount-point directory:
```
$ sudo mkdir /vol1
```
Add the following line to the end of `/etc/fstab`, so it will be mounted upon reboot:
```
LABEL=VOL1              /vol1   ext4    defaults        0 2
```
Then mount it by doing `sudo mount -a`, and confirm the available space by running `df -h` - the `/vol1`
filesystem should be listed.

### Install Language/Tool Packages

Install several development packages that aren't installed by default in the base system:
```
$ sudo apt install gcc make unzip
```

### Install Go, Protobuffers, and kubectl tools
Install the Go compiler and associated tools. Currently, the latest version is 1.20.5, but there may
be newer versions:

```
$ curl --location -O https://go.dev/dl/go1.20.5.linux-amd64.tar.gz
$ sudo tar -C /usr/local -xzvf go1.20.5.linux-amd64.tar.gl
$ echo 'export PATH=$PATH:/usr/local/go/bin' > go.sh
$ sudo cp go.sh /etc/profile.d/
```
Then, log out and back in again, then run `go version` to verify your path is now correct.

Install protoc:
```
$ curl -O --location https://github.com/protocolbuffers/protobuf/releases/download/v23.3/protoc-23.3-linux-x86_64.zip
$ cd /usr/local && sudo unzip ~/protoc-23.3-linux-x86_64.zip
$ cd ~
$ type protoc
```

Install kubectl:
```
$ curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
$ sudo cp kubectl /usr/local/bin
$ sudo chmod 755 /usr/local/bin/kubectl
$ kubectl version
```

### Install Docker

Warning: do not install Docker as provided by the `docker.io` and other packages in the Ubuntu base
packages repository - the version of Docker they provide is out-of-date.

Instead, follow the instructions for installing Docker on Ubuntu at https://docs.docker.com/engine/install/ubuntu/ .
Specifically, follow the listed steps for installing using an apt repository, and install the latest Docker version.

### Relocate Docker storage directory to secondary volume

Since Docker can use a lot of filesystem space, the directory where it stores container images, logs,
and other datafiles should be relocated to the separate, larger non-root volume on the system, so that
the root filesystem does not fill up.

Stop the Docker daemon(s) and copy the existing data directory to the new location:
```
$ sudo systemctl stop docker
$ ps ax | grep -i docker        # no Docker processes should be shown 

$ sudo rsync -av /var/lib/docker /vol1/
$ sudo rm -rf /var/lib/docker
$ sudo ln -s /vol1/docker /var/lib/docker
```
Then restart Docker and verify that it's working again:
```
$ sudo systemctl start docker  
$ sudo docker ps
$ sudo docker run hello-world
```

### Create user accounts, verify docker access

First, make a home directory parent in the new larger filesystem:
```
$ sudo mkdir /vol1/home
```
Then, for each user to be added, run the following steps - we will be using the account named 'testuser' here.
First, create the account and their home directory.
```
$ sudo adduser --shell /bin/bash --gecos 'Test User' --home /vol1/home/testuser testuser
```
Set up their $HOME/.ssh directory and add their SSH public-key:
```
$ sudo mkdir /vol1/home/testuser/.ssh
$ sudo vim /vol1/home/testuser/.ssh/authorized_keys
# In the editor, add the SSH public key string that the user has given you, save the file and exit
$ sudo chmod 600 /vol1/home/testuser/.ssh/authorized_keys
$ sudo chmod 700 /vol1/home/testuser/.ssh
$ sudo chown -R testuser:testuser /vol1/home/testuser/.ssh
```
Finally, add them to the `docker` group so they can run Docker commands without `sudo` access:
```
$ sudo gpasswd -a testuser docker
```
**sudo Access (OPTIONAL)**

If you want to give the new user `sudo` privileges, run the following command:
```
$ sudo gpasswd -a testuser sudo
```
