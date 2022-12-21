#!/bin/bash
set -exu
set -o pipefail

NAME=$1
PERF8=$2

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR="$SCRIPT_DIR/../.."
PLATFORM='unknown'
export REFRESH_RATE="${REFRESH_RATE:-5}"
export DATA_SIZE="${DATA_SIZE:-medium}"
PERF8=${PERF8:-$ROOT_DIR/bin/perf8}
PYTHON=${PYTHON:-$ROOT_DIR/bin/python}
ELASTIC_INGEST=${ELASTIC_INGEST:-$ROOT_DIR/bin/elastic-ingest}

unamestr=$(uname)
if [[ "$unamestr" == 'Linux' ]]; then
   PLATFORM='linux'
elif [[ "$unamestr" == 'Darwin' ]]; then
   PLATFORM='darwin'
fi


cd $ROOT_DIR/connectors/sources/tests/fixtures/$NAME

$PYTHON -m pip install -r requirements.txt

export RUNNING_FTEST=True

docker compose up -d

# XXX make run-stack should be blocking until everythign is up and running by checking hbs
sleep 30

$ROOT_DIR/bin/fake-kibana --index-name search-$NAME --service-type $NAME --debug

$PYTHON ./loadsample.py

if [[ $PERF8 == "yes" ]]
then
    if [[ $PLATFORM == "darwin" ]]
    then
      $PERF8 --refresh-rate $REFRESH_RATE -t $ROOT_DIR/perf8-ftest-report --asyncstats --memray --psutil -c $ELASTIC_INGEST --one-sync --sync-now --debug
    else
      $PERF8 --refresh-rate $REFRESH_RATE -t $ROOT_DIR/perf8-ftest-report --asyncstats --memray --pyspy --psutil -c $ELASTIC_INGEST --one-sync --sync-now --debug
    fi
else
    $ELASTIC_INGEST --one-sync --sync-now --debug
fi

$PYTHON ./remove.py

$ELASTIC_INGEST --one-sync --sync-now --debug
$PYTHON $ROOT_DIR/scripts/verify.py --index-name search-$NAME --service-type $NAME --size 3000

docker compose down --volumes
