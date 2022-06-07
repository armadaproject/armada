# EC2 Developer Setup

## Background

For easy development purposes, it is beneficial to setup a ec2 instance for development as the memory requirements for armada can be quite intense.  For example, a mac M1 with 8 gb is too small for development.  Some images will also not be supported on a M1 so it is beneficial to setup a ec2 instance for development.

## Instructions

- We suggest a t3.xlarge instance from aws ec2 with Amazon Linux.  16 GB of memory is suggested.
- During selection of instance, Add a large volume to your ec2 instance (100 gb)
- During selection of instance, add Security Group Rules that allows you to access tcp ports from your desktop browser.  This is useful for viewing lookout UI or other web services
- Install [docker](https://www.cyberciti.biz/faq/how-to-install-docker-on-amazon-linux-2/)
- Install docker-compose
- Install Go 
- Install Kind
- Install kubectl
- Install helm
- Install python3 (>= 3.7)
- Install dotnet for [linux](https://docs.microsoft.com/en-us/dotnet/core/install/linux-centos)
- We suggest using the remote code extension from VS Code.
- If you enable ports from your laptop, you can grab the DNS from your ec2 instance and replace localhost to access from the browser.  If you want to view lookout ui, you may need to set DANGEROUSLY_DISABLE_HOST_CHECK=true before running npm run start.
