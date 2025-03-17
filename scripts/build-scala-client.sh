#!/bin/bash
# This script is intended to be run under the docker container
# in the root dir of the Armada repo

ROOT=$(pwd)
SDIR=client/scala/armada-scala-client

# the mvn pom.xml in the $SDIR directory causes the following commands copy all relevant proto files
# into $SDIR/src/main/protobuf and compile these protobuf files into Scala files into $SDIR/target/generated-sources/scala-2.13/
# this then compils all Scala files into Java classes and packages everything into a jar file

cd $ROOT/$SDIR
mvn clean package

if [ $? -eq 0 ]; then
  jarfile=$(find . -type f -name 'armada-scala-client*.jar' | sort | head -n1)
  if [[ -e "$jarfile" ]]; then
    echo "" > /dev/stderr
    jarfile=$(echo $jarfile | sed -e s%^\./%%)
    echo "Armada Scala client jar file written to $SDIR/$jarfile" > /dev/stderr
  fi
else
  echo "mvn build exited with exit code $?" > /dev/stderr
fi

