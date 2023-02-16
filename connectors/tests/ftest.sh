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
export RUNNING_FTEST=True
export VERSION='8.7.0-SNAPSHOT'

PERF8_BIN=${PERF8_BIN:-$ROOT_DIR/bin/perf8}
PYTHON=${PYTHON:-$ROOT_DIR/bin/python}
ELASTIC_INGEST=${ELASTIC_INGEST:-$ROOT_DIR/bin/elastic-ingest}

unamestr=$(uname)
if [[ "$unamestr" == 'Linux' ]]; then
   PLATFORM='linux'
elif [[ "$unamestr" == 'Darwin' ]]; then
   PLATFORM='darwin'
fi


cd $ROOT_DIR/connectors/sources/tests/fixtures

if [ -f "$NAME/.env" ]; then
  echo "Loading env for $NAME"
  export $(grep -v '^#' $NAME/.env | xargs)
fi


if [ -f "$NAME/requirements.txt" ]; then
$PYTHON -m pip install -r $NAME/requirements.txt
fi
$PYTHON fixture.py --name $NAME --action setup
$PYTHON fixture.py --name $NAME --action start_stack
$ROOT_DIR/bin/fake-kibana --index-name search-$NAME --service-type $NAME --debug --filtering $NAME/filtering.json
$PYTHON fixture.py --name $NAME --action load
$PYTHON fixture.py --name $NAME --action sync

if [[ $PERF8 == "yes" ]]
then
    if [[ $PLATFORM == "darwin" ]]
    then
      $PERF8_BIN --refresh-rate $REFRESH_RATE -t $ROOT_DIR/perf8-report-$NAME --asyncstats --memray --psutil -c $ELASTIC_INGEST --debug & PID=$!
    else
      $PERF8_BIN --refresh-rate $REFRESH_RATE -t $ROOT_DIR/perf8-report-$NAME --asyncstats --memray --psutil -c $ELASTIC_INGEST --debug & PID=$!
    fi
else
    $ELASTIC_INGEST --debug & PID=$!
fi

$PYTHON fixture.py --name $NAME --action monitor --pid $PID

$PYTHON fixture.py --name $NAME --action remove
$PYTHON fixture.py --name $NAME --action sync

$ELASTIC_INGEST --debug & PID=$!

$PYTHON fixture.py --name $NAME --action monitor --pid $PID

NUM_DOCS=`$PYTHON fixture.py --name $NAME --action get_num_docs`
$PYTHON $ROOT_DIR/scripts/verify.py --index-name search-$NAME --service-type $NAME --size $NUM_DOCS
$PYTHON fixture.py --name $NAME --action stop_stack
$PYTHON fixture.py --name $NAME --action teardown
