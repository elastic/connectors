#!/bin/bash
set -exuo pipefail

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install ca-certificates curl gnupg lsb-release -y
sudo mkdir -p /etc/apt/keyrings

echo "Installing Docker & Docker Compose"
ARCH=`dpkg --print-architecture`
RELEASE=`lsb_release -cs`
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$ARCH signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $RELEASE stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo systemctl start docker

echo "Starting test task"
BASEDIR=$(realpath $(dirname $0))
ROOT=$(realpath $BASEDIR/../)

cd $ROOT

# docker snapshot publication
make docker-build
make docker-push
