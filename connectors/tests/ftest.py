import os
import sys
import argparse
import time


ROOT_DIR = os.path.join(os.path.dirname(__file__), '..', '..')


name = sys.argv[1]
if len(sys.argv) > 2:
    BIN_DIR = sys.argv[2]
else:
    BIN_DIR = os.path.join(ROOT_DIR, 'bin')

# start stack
curdir = os.getcwd()
os.chdir(os.path.join(ROOT_DIR, 'connectors', 'sources', 'tests', 'fixtures',
                      name))

os.system('make run-stack')
# XXX make run-stack should be blocking until everythign is up and running by checking hbs
time.sleep(60)

try:
    os.system(f"{BIN_DIR}/fake-kibana --index-name search-{name} --service-type {name} --debug")
    os.system('make load-data')
    os.system(f"{BIN_DIR}/elastic-ingest --one-sync --sync-now")
    os.system(f"{BIN_DIR}/elastic-ingest --one-sync --sync-now")
    os.system(f"{BIN_DIR}/python {ROOT_DIR}/scripts/verify.py --index-name search-{name} --service-type {name} --size 3000")
finally:
    os.system('make stop-stack')

