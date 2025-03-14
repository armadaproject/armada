# Armada Java gRPC Client

This is the Java client for interacting with the Armada gRPC API.

## Requirements

- **Java 11** (required by Jenkins K8s plugin)
- **Maven 3.9.9**

## Quick Start

### Build
From the root of the Armada repository, run:
```bash
mage BuildJava
```

### Run Example
Example programs are located in the [examples](src/main/java/io/armadaproject/examples/) directory.

Ensure an **Armada server** is running on `localhost:30002` before executing the example:
```bash
mvn exec:java -Dexec.mainClass="io.armadaproject.examples.SubmitJob"
```

## Development

### Local Build
```bash
mvn clean package
```

### Run Tests
```bash
mvn clean test
```

## Troubleshooting

### IntelliJ Not Recognizing Generated Sources
If IntelliJ doesn't recognize generated sources, update the IDE settings:

1. Go to **Help > Edit Custom Properties**
2. Add the following line:
   ```
   idea.max.intellisense.filesize=20000
   ```
3. Restart IntelliJ


