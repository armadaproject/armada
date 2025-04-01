FROM eclipse-temurin:11-jdk

ARG MAVEN_VERSION=3.9.9

LABEL org.opencontainers.image.authors="G-Research Open-Source Software"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.ref.name="Java 11 Client Builder"
LABEL org.opencontainers.image.version=""

RUN set -ex && \
    apt update && \
    apt install -y software-properties-common && \
    apt-add-repository -y ppa:git-core/ppa && \
    apt update && \
    apt install -y curl git && \
    # Download and install Maven
    curl -s -O https://downloads.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz && \
    tar -C /opt -xzvf apache-maven-${MAVEN_VERSION}-bin.tar.gz && \
    ln -s /opt/apache-maven-${MAVEN_VERSION}/bin/mvn /usr/local/bin/mvn && \
    # Clean up
    apt-get clean && \
    rm -rf apache-maven-${MAVEN_VERSION}-bin.tar.gz /var/lib/apt/lists/*


COPY scripts/build-java-client.sh /
RUN chmod +x /build-java-client.sh

ENTRYPOINT [ "/build-java-client.sh" ]
