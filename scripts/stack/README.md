# Full Elastic Stack with Connectors

### Prerequisites


### Script commands

#### run-stack

`scripts/stack/run-stack.sh`

Command line options:
* `-u | --update-images`: perform a fresh pull on the docker images
* `-n | --no-connectors`: do not run or configure connectors
* `-x | --no-configuration`: do not ask to run the connectors configuration

#### stop-stack

`scripts/stack/stop-stack.sh`

Command line options:
* `-v | --remove-volumes`: delete all data volumes on stop

### Prompted Configuration

If you run the `run-stack.sh` command without any flags, by default the script will
ask if you want to set up the connectors configuration. If you enter "y" to run
the configurator, it will take you through a set of prompts using the
[Connectors CLI](../../connectors/connectors_cli.py) to create a new index and connector.

The resulting configuration will be saved to the [config.yml](./connectors-config/config.yml)
file that is used to start the Connectors container.

### Manual Configuration

1. ensure your environment variables are set in `.env`
2. First time run - set up a connector
    1. run `run-stack.sh --no-connectors`
    2. Content->Indices->Create a new Index
    3. Choose "Connector"
    4. Select the connector type
    5. Set the index name
    6. Generate API Key
    7. Set Connector name and description (optional)
    8. Copy the resulting connector_id, service_type, and api_key for your `connectors-config/config.yml`
    9. when complete, temporarily stop the stack via `stop-stack.sh`
3. edit the file in `connectors-config/config.yml`
4. run `run-stack.sh` again, and go back to your connector
   1. Content->indices
   2. click on your index / connector name
   3. go to the "configuration" tab
   4. Complete the configuration and sync
