#!/bin/bash
set -euo pipefail

sudo apt-get update
sudo apt-get install ca-certificates curl gnupg lsb-release -y
sudo mkdir -p /etc/apt/keyrings

ARCH=`dpkg --print-architecture`
RELEASE=`lsb_release -cs`
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$ARCH signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $RELEASE stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo systemctl start docker


BASEDIR=$(realpath $(dirname $0))
ROOT=$(realpath $BASEDIR/../)

cd $ROOT/connectors/sources/tests/fixtures/mysql

export DATA_SIZE=small

make run-stack
sleep 120

make load-data

cd $ROOT
docker run --rm -v $ROOT:/ci -w=/ci \
    -it \
    python:3.10 \
    /bin/bash -c  "/ci/.buildkite/nightly.sh"

cd $ROOT/connectors/sources/tests/fixtures/mysql
make stop-stack
