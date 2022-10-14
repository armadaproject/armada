#! /bin/bash

# this bit of code will allow the host machine's .kube/config to
# work inside a container.
#
# it presumes that host ~/.kube dir is mounted to /tmp/.kube in
# the container
#
# what this does is create a hosts file entry for 'kubernetes.default'
# with the same IP as host.docker.internal. at this address, we can use
# the host machine's kube control plane.

if [ -f /.dockerenv ]; then
    DOCKER_HOST=$(ping -c 1 host.docker.internal | grep -m 1 -o -E '[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}')
    echo "$DOCKER_HOST kubernetes.default" >> /etc/hosts
    cp -R /tmp/.kube ~
    sed -i s/127.0.0.1/kubernetes.default/ ~/.kube/config
fi
