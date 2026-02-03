#!/bin/bash
set -e

# Configure Git if environment variables are set
if [ -n "$GIT_USER_NAME" ]; then
    echo "Configuring git user.name: $GIT_USER_NAME"
    git config --global user.name "$GIT_USER_NAME"
fi

if [ -n "$GIT_USER_EMAIL" ]; then
    echo "Configuring git user.email: $GIT_USER_EMAIL"
    git config --global user.email "$GIT_USER_EMAIL"
fi

if [ -n "$GIT_SIGNING_KEY" ]; then
    echo "Configuring git SSH signing"
    git config --global user.signingkey "$GIT_SIGNING_KEY"
    git config --global gpg.format ssh
    git config --global commit.gpgsign true
    
    # Set up allowed signers file for signature verification
    mkdir -p ~/.ssh
    if [ -n "$GIT_USER_EMAIL" ]; then
        echo "$GIT_USER_EMAIL $GIT_SIGNING_KEY" > ~/.ssh/allowed_signers
        git config --global gpg.ssh.allowedSignersFile ~/.ssh/allowed_signers
        echo "Created allowed signers file for signature verification"
    fi
fi

# Fix permissions on node_modules
echo "Setting permissions on node_modules..."
sudo chown -R vscode:vscode $PWD/internal/lookoutui/node_modules
sudo chown -R vscode:vscode $PWD/website/node_modules

# Download Go modules
echo "Downloading Go modules..."
sudo chown -R vscode:vscode /go/pkg
go mod download

# Install goreman for local development https://github.com/armadaproject/armada?tab=readme-ov-file#local-development-with-goreman
go install github.com/mattn/goreman@latest

echo "Post-create setup complete!"
