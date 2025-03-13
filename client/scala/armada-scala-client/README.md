# Armada Scala client

## Build

You can build the Scala Armada client simply with `mvn package`.

The jar can be found in `target/`.

You can compile with a different Scala version as follows:

    ./set-scala-version.sh 2.12.18

Then build the project as before.

### Build in isolation

Alternatively, you can build this project isolated from your system in a Docker container as follows:

    # go to the root of this repository
    cd ../../..

    # Go and Docker needs to be installed, the rest is isolated
    go run github.com/magefile/mage@v1.14.0 -v buildScala

## Notes

If your IDE cannot find the Scala classes generated from protobuf files, you have to run `mvn compile` on the command line first.

