# EC2 Developer Setup

## Background

For development, you might want to set up an Amazon EC2 instance as the resource requirements for Armada are substantial. A typical Armada installation requires a system with at least 16GB of memory to perform well. Running Armada on a laptop made before ~2017 will typically eat battery life and result in a slower UI.

Note: As of June 2022, not all Armada dependencies reliably build on a Mac M1 using standard package management. So if you have an M1 Mac, working on EC2 or another external server is your best bet.

## Instructions

- We suggest a t3.xlarge instance from aws ec2 with AmazonLinux as the OS.  16 GB of memory is suggested.
- During selection of instance, Add a large volume to your ec2 instance.   100 gb of storage is recommended.
- When selecting the instance, you will have the opportunity to choose a security group. You may need to make a new one. Be sure to add a rule allowing inbound communication on port 22 so that you can access your server via SSH. We recommend that you restrict access to the IP address from which you access the Internet, or a small CIDR block containing it.

If you want to use your browser to access Armada Lookout UI or other web-based interfaces, you will also need to grant access to their respective ports. For added security, consider using an [SSH tunnel](https://www.ssh.com/academy/ssh/tunneling/example) from your local machine to your development server instead of opening those ports. You can add LocalForward to your ssh config: `LocalForward 4000 localhost:3000`  
- Install [docker](https://www.cyberciti.biz/faq/how-to-install-docker-on-amazon-linux-2/)
- Install docker-compose
- Install Go 
- Install Kind
- Install kubectl
- Install helm
- Install python3 (>= 3.7)
- Install dotnet for [linux](https://docs.microsoft.com/en-us/dotnet/core/install/linux-centos)
- We suggest using the [remote code extension] (https://code.visualstudio.com/docs/remote/ssh) for VS Code if that is your IDE of choice.
- Simplest way to setup development environment is to run docs/dev/setup.sh
