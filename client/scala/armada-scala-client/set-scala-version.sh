#!/bin/bash

if [ $# -eq 1 ]
then
    scala=$1

    scala_compat=${scala%.*}
    scala_major=${scala_compat%.*}
    scala_minor=${scala_compat/*./}
    scala_patch=${scala/*./}

    echo "setting scala=$scala"
    sed -i -E \
        -e "s%^(  <artifactId>)([^_]+)[_0-9.]+(</artifactId>)$%\1\2_${scala_compat}\3%" \
        -e "s%^(    <scala.major.version>).+(</scala.major.version>)$%\1${scala_major}\2%" \
        -e "s%^(    <scala.minor.version>).+(</scala.minor.version>)$%\1${scala_minor}\2%" \
        -e "s%^(    <scala.patch.version>).+(</scala.patch.version>)$%\1${scala_patch}\2%" \
        pom.xml
else
    echo "Provide the Scala version to set"
    exit 1
fi

