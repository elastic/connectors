#!/bin/bash
set -exu
set -o pipefail

NAME=$1
PERF8=$2

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR="$SCRIPT_DIR/../.."
PLATFORM='unknown'
MAX_RSS="200M"

export REFRESH_RATE="${REFRESH_RATE:-5}"
export DATA_SIZE="${DATA_SIZE:-medium}"
export RUNNING_FTEST=True
export VERSION='8.8.0-SNAPSHOT'

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
$ROOT_DIR/bin/fake-kibana --index-name search-$NAME --service-type $NAME --connector-definition $NAME/connector.json --debug
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

$ELASTIC_INGEST --debug & PID_2=$!

$PYTHON fixture.py --name $NAME --action monitor --pid $PID_2

NUM_DOCS=`$PYTHON fixture.py --name $NAME --action get_num_docs`
$PYTHON $ROOT_DIR/scripts/verify.py --index-name search-$NAME --service-type $NAME --size $NUM_DOCS
$PYTHON fixture.py --name $NAME --action stop_stack
$PYTHON fixture.py --name $NAME --action teardown

# Wait for PERF8 to compile the report
# Actual report compilation starts right when the first sync finishes, but happens in the background
# So we wait in the end of the script to not block second sync from happening while we also compile the report
if [[ $PERF8 == "yes" ]]; then
    set +e
    echo 'Waiting for PERF8 to finish the report'
    PERF8_PID=`ps aux | grep bin/perf8 | grep -v grep | awk '{print $2}'`
    while kill -0 "$PERF8_PID"; do
        sleep 0.5
    done
    set -e

    # reading the status to know if we need to fail
    STATUS=$(<$ROOT_DIR/perf8-report-$NAME/status)
    exit $STATUS
fi


