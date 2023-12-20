#!/bin/bash

parse_params() {
  update_images=false
  remove_volumes=false
  no_connectors=false
  connectors_only=false
  bypass_config=false
  reset_config=-false

  #Boilerplate parameter parsing
  PARAMS=""
  while (( "$#" )); do
    case "$1" in
      -n|--no-connectors)
        no_connectors=true
        shift 1
        ;;
      -x|--no-configuration)
        bypass_config=true
        shift 1
        ;;
      -c|--connectors-only)
        connectors_only=true
        shift 1
        ;;
      -u|--update-images)
        update_images=true
        shift 1
        ;;
      -v|--remove-volumes)
        remove_volumes=true
        shift 1
        ;;
      -r|--reset-configuration)
        reset_config=true
        shift 1
        ;;
      --) # end argument parsing
        shift
        break
        ;;
      -*|--*=) # unsupported flags
        echo "Error: Unsupported flag $1" >&2
        exit 1
        ;;
      *) # preserve positional arguments
        PARAMS="$PARAMS $1"
        shift
        ;;
    esac
  done


  parsed_params=$PARAMS
}
