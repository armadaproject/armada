# EC2 Developer Setup

## Background

For easy development purposes, it is beneficial to setup a ec2 instance for development as the memory requirements for armada can be quite intense.  For example, a mac M1 with 8 gb is too small for development.  Some images will also not be supported on a M1 so it is beneficial to setup a ec2 instance for development.

## Instructions

- We suggest a t3.large instance from aws ec2 with Amazon Linux.  8 GB of memory is suggested.
- Install [docker](https://www.cyberciti.biz/faq/how-to-install-docker-on-amazon-linux-2/)
- Install Go
- Install Kind
- Install kubectl
- Install helm
- Add a Elastic Block Storage (EBS) volume to the ec2 instance.  Default storage in the instance is not enough. [Tutorial](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-creating-volume.html)
- 
