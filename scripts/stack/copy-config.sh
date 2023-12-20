#!/bin/bash

CONFIG_PATH=${1:-}

if [[ ${CURDIR:-} == "" ]]; then
    export CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
fi
source $CURDIR/read-env.sh $CURDIR/.env

config_dir="$PROJECT_ROOT/scripts/stack/connectors-config"
script_config="$config_dir/config.yml"

if [ -f "$script_config" ]; then
    echo "config.yml already exists in $config_dir. Not overwriting."
    exit 0
fi

is_example_config=false
if [[ "${CONFIG_PATH:-}" == "" ]]; then
    CONFIG_PATH="$PROJECT_ROOT/config.yml"
    is_example_config=true
fi

mkdir -p "$config_dir"

cp "$CONFIG_PATH" "$script_config"
echo "copied config from $CONFIG_PATH to $config_dir"

if [[ "$is_example_config" == true ]]; then
    sed_cmd="sed -i"
    if [[ "$MACHINE_OS" == "MacOS" || "$MACHINE_OS" == "FreeBSD" ]]; then
        sed_cmd="sed -i -e"
    fi
    $sed_cmd '/connectors:/s/^#//g' "$script_config"
    $sed_cmd '/elasticsearch.host/s/^#//g' "$script_config"
    $sed_cmd '/elasticsearch.username/s/^#//g' "$script_config"
    $sed_cmd '/elasticsearch.password/s/^#//g' "$script_config"
fi
