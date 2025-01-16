#!/bin/bash
# This script is intended to be run under the docker container at $ARMADADIR/build/scala-api-client/

export PATH=/sbt/bin:$PATH

cd client/scala/scala-armada-client
sbt clean && sbt compile

