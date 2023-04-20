#!/bin/bash

# TODO: convert all this install in a docker image we can just use
set -exuo pipefail

make ftest NAME=dir DATA_SIZE=small
