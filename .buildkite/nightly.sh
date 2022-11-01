#!/bin/bash

apt-get update

apt-get install ca-certificates curl gnupg lsb-release -y

mkdir -p /etc/apt/keyrings

ARCH=`dpkg --print-architecture`
RELEASE=`lsb_release -cs`

curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$ARCH signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $RELEASE stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

apt-get update

apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
systemctl start docker

python -m venv dockerp
/ci/dockerp/bin/pip install -r requirements.txt
/ci/dockerp/bin/python setup.py develop

export PYTHON=/ci/dockerp/bin/python

env mysql.host=host.docker.internal env elasticsearch.host=http://host.docker.internal:9200 /ci/dockerp/bin/python /ci/connectors/tests/ftest.py mysql /ci/dockerp/bin
