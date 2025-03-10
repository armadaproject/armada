#!/bin/bash
# This script is intended to be run under the docker container at $ARMADADIR/build/java-client

ROOT=$(pwd)
JDIR=client/java
cd $ROOT/$JDIR

# Build the Java client using Maven
mvn clean install

if [ $? -eq 0 ]; then
  jarfile=$(find target -type f -name 'java-client*.jar')
  if [[ $jarfile ]]; then
    echo "" > /dev/stderr
    jarfile=$(echo $jarfile | sed -e s%^\./%%)
    echo "Armada Java client jar file written to $JDIR/$jarfile" > /dev/stderr
  fi
else
  echo "Maven build exited with exit code $?" > /dev/stderr
fi
