#!/bin/bash
# This script is intended to be run under the docker container
# in the root dir of the Armada repo

export PATH=/sbt/bin:$PATH

ROOT=$(pwd)
SDIR=client/scala/scala-armada-client

rm -rf $ROOT/$SDIR/proto
mkdir -p $ROOT/$SDIR/proto

cd proto
for pfile in \
    google/api/annotations.proto \
    google/api/http.proto \
    google/protobuf/*.proto \
    github.com/gogo/protobuf/gogoproto/gogo.proto \
    k8s.io/api/core/v1/generated.proto \
    k8s.io/apimachinery/pkg/api/resource/generated.proto \
    k8s.io/apimachinery/pkg/apis/meta/v1/generated.proto \
    k8s.io/apimachinery/pkg/runtime/generated.proto \
    k8s.io/apimachinery/pkg/runtime/schema/generated.proto \
    k8s.io/apimachinery/pkg/util/intstr/generated.proto \
    k8s.io/api/networking/v1/generated.proto
do
  dir=$(dirname $pfile)
  mkdir -p $ROOT/$SDIR/proto/$dir
  cp $pfile $ROOT/$SDIR/proto/$dir/
done

cd ..
for pfile in \
    pkg/api/event.proto pkg/api/submit.proto pkg/api/health.proto pkg/api/job.proto pkg/api/binoculars/binoculars.proto
do
  dir=$(dirname $pfile)
  mkdir -p $ROOT/$SDIR/proto/$dir
  cp $pfile $ROOT/$SDIR/proto/$dir/
done

# the sbt config in the $SDIR directory causes the following commands to generate
# the scala protobuf files into $SDIR/target/scala-2.13/src_managed/main/

cd $ROOT/$SDIR
sbt clean && \
sbt -Dsbt.io.implicit.relative.glob.conversion=allow compile && \
sbt -Dsbt.io.implicit.relative.glob.conversion=allow package

if [ $? -eq 0 ]; then
  jarfile=$(find . -type f -name 'scala-armada-client*.jar')
  if [[ $jarfile ]]; then
    echo "" > /dev/stderr
    jarfile=$(echo $jarfile | sed -e s%^\./%%)
    echo "Armada Scala client jar file written to $SDIR/$jarfile" > /dev/stderr
  fi
else
  echo "sbt build exited with exit code $?" > /dev/stderr
fi

