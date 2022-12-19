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

unamestr=$(uname)
if [[ "$unamestr" == 'Linux' ]]; then
   PLATFORM='linux'
elif [[ "$unamestr" == 'Darwin' ]]; then
   PLATFORM='darwin'
fi


cd $ROOT_DIR/connectors/sources/tests/fixtures/$NAME
make run-stack

# XXX make run-stack should be blocking until everythign is up and running by checking hbs
sleep 30

$ROOT_DIR/bin/fake-kibana --index-name search-$NAME --service-type $NAME --debug

make load-data

if [[ $PERF8 == "yes" ]]
then
    if [[ $PLATFORM == "darwin" ]]
    then
      $ROOT_DIR/bin/perf8 --refresh-rate $REFRESH_RATE -t $ROOT_DIR/perf8-ftest-report --asyncstats --memray --psutil -c $ROOT_DIR/bin/elastic-ingest --one-sync --sync-now --debug
    else
      $ROOT_DIR/bin/perf8 --refresh-rate $REFRESH_RATE -t $ROOT_DIR/perf8-ftest-report --asyncstats --memray --pyspy --psutil -c $ROOT_DIR/bin/elastic-ingest --one-sync --sync-now --debug
    fi
else
    $ROOT_DIR/bin/elastic-ingest --one-sync --sync-now --debug
fi

make remove-data

$ROOT_DIR/bin/elastic-ingest --one-sync --sync-now --debug
$ROOT_DIR/bin/python $ROOT_DIR/scripts/verify.py --index-name search-$NAME --service-type $NAME --size 3000

make stop-stack
