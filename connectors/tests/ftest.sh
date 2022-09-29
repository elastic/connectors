#!/bin/bash
set -exu
set -o pipefail

NAME=$1
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR="$SCRIPT_DIR/../.."

cd $ROOT_DIR/connectors/sources/tests/fixtures/$NAME
make run-stack
# XXX make run-stack should be blocking until everythign is up and running by checking hbs
sleep 30

$ROOT_DIR/bin/fake-kibana --index-name search-$NAME --service-type $NAME --debug

make load-data

$ROOT_DIR/bin/elastic-ingest --one-sync --sync-now
$ROOT_DIR/bin/elastic-ingest --one-sync --sync-now
$ROOT_DIR/bin/python $ROOT_DIR/scripts/verify.py --index-name search-$NAME --service-type $NAME --size 3000

make stop-stack
