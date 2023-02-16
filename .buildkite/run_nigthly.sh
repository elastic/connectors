#!/bin/bash
set -exuo pipefail

# XXX convert all this install in a docker image we can just use
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

# installs Python 3.10
echo "Installing Python 3.10"
sudo apt-get remove python3-pip python3-setuptools -y
sudo DEBIAN_FRONTEND=noninteractive apt install software-properties-common -y
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo TZ=UTC DEBIAN_FRONTEND=noninteractive apt install --no-install-recommends python3.10 python3.10-dev python3.10-venv -y


curl -sS https://bootstrap.pypa.io/get-pip.py | sudo python3.10

echo "Starting test task"
BASEDIR=$(realpath $(dirname $0))
ROOT=$(realpath $BASEDIR/../)

cd $ROOT

/usr/bin/python3.10 -m venv .
export PERF8_BIN=$ROOT/bin/perf8
export PYTHON=$ROOT/bin/python
export PIP=$ROOT/bin/pip
export DATA_SIZE=small
export VERSION=8.7.0-SNAPSHOT

$PIP install -r requirements/tests.txt
$PIP install -r requirements/x86_64.txt
$PYTHON setup.py develop
$PIP install py-spy

# running the e2e test
connectors/tests/ftest.sh $1 yes
