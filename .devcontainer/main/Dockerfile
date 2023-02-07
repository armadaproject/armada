FROM codercom/enterprise-base:ubuntu

ENV GOROOT /usr/local/go
ENV GOPATH /home/codespace/go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

ENV DEBIAN_FRONTEND noninteractive

USER root

# Install apt packages
RUN add-apt-repository ppa:deadsnakes/ppa &&\
    apt-get update &&\
    apt-get install -y nano python3.9 &&\
    # Cleanup
    apt-get clean &&\
    rm -rf /var/lib/apt/lists/*

# Golang Install 1.19.4
RUN wget https://dl.google.com/go/go1.19.4.linux-amd64.tar.gz && \
    tar -xvf go1.19.4.linux-amd64.tar.gz && \
    mv go /usr/local && \
    rm go1.19.4.linux-amd64.tar.gz && \
# Install go 1.18.9
    go install golang.org/dl/go1.18.9@latest && \
    go1.18.9 download && \
    rm -rf /root/sdk/go1.18.9/go1.18.9.linux-amd64.tar.gz

# Python 3.9 install
RUN python3.9 -m pip install pipenv pylint flake8 black mypy && \
    curl -sSL https://install.python-poetry.org | python3 -

# Kubectl
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" &&\
    sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl &&\
    rm kubectl

# Install whichever Node version is LTS
RUN curl -sL https://deb.nodesource.com/setup_lts.x | bash - &&\
    curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - &&\
    echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list

# Install Node and Yarn
RUN apt-get update && apt-get install -y yarn nodejs && \
    # Cleanup
    apt-get clean &&\
    rm -rf /var/lib/apt/lists/*

RUN chown -R codespace:codespace /home/codespace

# Create codespace user
RUN useradd -m codespace &&\
    chown -R codespace:codespace /home/codespace

USER codespace
WORKDIR /home/codespace/

RUN git clone https://github.com/armadaproject/armada.git

# Set default shell to bash
ENV SHELL /bin/bash



