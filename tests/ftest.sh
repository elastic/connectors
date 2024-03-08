#!/bin/bash
set -exu
set -o pipefail

NAME=$1
PERF8=$2

# add a flag for serverless
if [[ "$NAME" == *"serverless"* ]]; then
  export SERVERLESS="yup"
fi

SERVICE_TYPE=${NAME%"_serverless"}
INDEX_NAME=search-${NAME%"_serverless"}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR="$SCRIPT_DIR/.."
PLATFORM='unknown'
MAX_RSS="200M"
MAX_DURATION=600

export PERF8_TRACE=${PERF8_TRACE:-False}
export REFRESH_RATE="${REFRESH_RATE:-5}"
export DATA_SIZE="${DATA_SIZE:-medium}"
export RUNNING_FTEST=True
export VERSION='8.12.0-SNAPSHOT'

if [ "$PERF8_TRACE" == true ]; then
    echo 'Tracing is enabled, memray stats will be delivered'
    PLUGINS='--asyncstats --memray --psutil'
else
    PLUGINS='--asyncstats --psutil'
fi

PERF8_BIN=${PERF8_BIN:-$ROOT_DIR/bin/perf8}
PYTHON=${PYTHON:-$ROOT_DIR/bin/python}
ELASTIC_INGEST=${ELASTIC_INGEST:-$ROOT_DIR/bin/elastic-ingest}

unamestr=$(uname)
if [[ "$unamestr" == 'Linux' ]]; then
   PLATFORM='linux'
elif [[ "$unamestr" == 'Darwin' ]]; then
   PLATFORM='darwin'
fi


cd $ROOT_DIR/tests/sources/fixtures

if [ -f "$NAME/.env" ]; then
  echo "Loading env for $NAME"
  export $(grep -v '^#' $NAME/.env | xargs)
fi


if [ -f "$NAME/requirements.txt" ]; then
$PYTHON -m pip install -r $NAME/requirements.txt
fi
$PYTHON fixture.py --name $NAME --action setup
$PYTHON fixture.py --name $NAME --action start_stack
$PYTHON fixture.py --name $NAME --action check_stack
$ROOT_DIR/bin/fake-kibana --index-name $INDEX_NAME --service-type $SERVICE_TYPE --config-file $NAME/config.yml --connector-definition $NAME/connector.json --debug
$PYTHON fixture.py --name $NAME --action load

if [[ $PERF8 == "yes" ]]
then
    $PYTHON fixture.py --name $NAME --action description > description.txt
    if [[ $PLATFORM == "darwin" ]]
    then
      $PERF8_BIN --refresh-rate $REFRESH_RATE -t $ROOT_DIR/perf8-report-$NAME $PLUGINS --max-duration $MAX_DURATION --description description.txt -c $ELASTIC_INGEST --config-file $NAME/config.yml --debug & PID=$!
    else
      $PERF8_BIN --refresh-rate $REFRESH_RATE -t $ROOT_DIR/perf8-report-$NAME $PLUGINS --max-duration $MAX_DURATION --description description.txt -c $ELASTIC_INGEST --config-file $NAME/config.yml --debug & PID=$!
    fi
else
    $ELASTIC_INGEST --config-file $NAME/config.yml --debug & PID=$!
fi


$PYTHON fixture.py --name $NAME --action monitor --pid $PID

$PYTHON fixture.py --name $NAME --action remove

$ELASTIC_INGEST  --config-file $NAME/config.yml  --debug & PID_2=$!

$PYTHON fixture.py --name $NAME --action monitor --pid $PID_2


NUM_DOCS=`$PYTHON fixture.py --name $NAME --action get_num_docs`
$PYTHON $ROOT_DIR/scripts/verify.py --index-name $INDEX_NAME --service-type $NAME --size $NUM_DOCS
$PYTHON fixture.py --name $NAME --action teardown

# stopping the stack as a final step once everything else is done.
$PYTHON fixture.py --name $NAME --action stop_stack

# Wait for PERF8 to compile the report
# Actual report compilation starts right when the first sync finishes, but happens in the background
# So we wait in the end of the script to not block second sync from happening while we also compile the report
if [[ $PERF8 == "yes" ]]; then
    set +e
    PERF8_PID=`ps aux | grep bin/perf8 | grep -v grep | awk '{print $2}'`
    if [ ! -z "$PERF8_PID" ] # if the process is already gone, move on
    then
      echo 'Waiting for PERF8 to finish the report'
      while kill -0 "$PERF8_PID"; do
          sleep 0.5
      done
    fi
    set -e

    rm -f description.txt

    # reading the status to know if we need to fail
    STATUS=$(<$ROOT_DIR/perf8-report-$NAME/status)
    exit $STATUS
fi

# make sure the ingest processes are terminated
set +e # if the PID disappears right before the kill, that's not an error
if ps -p $PID > /dev/null
then
  echo 'Killing the ingest process'
  kill -TERM $PID
  sleep 5
  if ps -p $PID > /dev/null
  then
    kill -KILL $PID
  fi
fi

if ps -p $PID_2 > /dev/null
then
  echo 'Killing the second ingest process'
  kill -TERM $PID_2
  sleep 5
  if ps -p $PID_2 > /dev/null
  then
    kill -KILL $PID_2
  fi
fi
set -e
