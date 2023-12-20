# Full Elastic Stack with Connectors

We know that setting up the Connectors service may look a bit daunting, but to help with this, we pulled together a set of scripts to enable a user to get up and running with a full stack with connectors. Here, we provide an example configuration, Docker Compose configuration, and a bash script to startup and run a stack including Elasticsearch, Kibana, and Connectors.

These scripts can be useful if you want to try out Connectors and ingesting their data into an Elasticsearch instance, or, as a starting place for adding Connectors to your production environment.

# Contents

* [Prerequisites](#prerequisites)
* [Running the Stack](#running-the-stack)
* [Prompted Configuration](#prompted-configuration)
* [Manual Configuration](#manual-configuration)
* [Script command reference](#script-command-reference)
  * [run-stack.sh](#run-stacksh)
  * [stop-stack.sh](#stop-stacksh)

## Prerequisites
* Linux or MacOS (Windows is not currently supported)
* Docker with Docker Compose Installed

## Running the Stack

If you do not wish to use the default Elasticsearch username and password, you must set the username and password (or the API key)
in the default [connectors config.yml](./connectors-config/config.yml) file before running the `run-stack.sh` script. You can also
use a different password for the `elastic` user by specifying the environment variable `ELASTIC_PASSWORD` before running the script. E.g.:

```bash
$ ELASTIC_PASSWORD="my_new_password" ./scripts/stack/run-stack.sh
```

## Prompted Configuration

If you run the `run-stack.sh` command without any flags, by default the script will
ask if you want to set up the connectors configuration. If you enter "y" to run
the configurator, it will take you through a set of prompts using the
[Connectors CLI](../../connectors/connectors_cli.py) to create a new index and connector.

The resulting configuration will be saved to the [config.yml](./connectors-config/config.yml)
file that is used to start the Connectors container. Note that using the configurator,
it will modify the [connectors config.yml](./connectors-config/config.yml) file. You can restore it
by copying over the [example config.yml.example](./connectors-config/config.yml.example) file
to `config.yml` in the directory.

If you configured a new connector, you will need to go into Kibana to start or schedule a sync.
To do this, in Kibana:
* From the main menu, choose "Content" under the "Search" heading
* Click on your index name
* Click on the "Configuration" tab
  * Note, the API key will be already pre-configured, and you do not need to generate a new one
  * Optionally, change the name or add a description
  * Ensure your connector settings are the way you want, if not edit them to their correct values
  * Finally, set your schedule and sync

## Manual Configuration

Instead of using the CLI based configurator while running the `run-stack.sh`
script, you can choose to manually provide your connectors configurations and
use those instead. To do this and still use the `run-stack.sh` script:

1. Run the stack without the connectors service:
    1. Run `./scripts/stack/run-stack.sh --no-connectors` to run the full stack without the Connectors service
    2. From the main menu, go to "Content", "Indices", and create a new index for your connector
    3. Choose the "Connector" index type
    4. Select the connector type
    5. Set the index name
    6. Generate a new API Key, and copy it somewhere safe
    7. Set Connector name and description (optional)
    8. Copy the resulting connector_id, service_type, and api_key into a new entry in the `./scripts/stack/connectors-config/config.yml` fille
    9. Repeat as necessary for all the connectors you wish too create
2. when complete, temporarily stop the stack via `./scripts/stack/stop-stack.sh`
3. run `./scripts/stack/run-stack.sh --no-configuration` again to run the full stack without prompting for configuration.
    1. From the main menu, choose "Content" under the "Search" heading
    2. Click on your index name
    3. Click on the "Configuration" tab
    4. Complete the configuration and optionally start your sync
    5. Repeat these steps for all the connectors you created

## Script command reference

### run-stack.sh

This is the main script that sets up and runs an Elasticsearch, Kibana, and Connectors stack.

```bash
$ ./scripts/stack/run-stack.sh
```

By default, the script will start an Elasticsearch and Kibana instance, then it will ask the user if they want
to configure the connectors, and finally start the connectors service.

Command line options:
* `-u | --update-images`: perform a fresh pull on the docker images
* `-n | --no-connectors`: do not run the connectors service or the connectors configuration
* `-x | --no-configuration`: do not ask to run the connectors configuration, but still run the service
* `-c | --connectors-only`: only start the connectors service. Useful if Elasticsearch and Kibana are already running

### stop-stack.sh

This script is used to stop the stack, and optionally remove any data volumes and reset the configuration file.

```bash
$ ./scripts/stack/stop-stack.sh
```

Command line options:
* `-v | --remove-volumes`: delete all data volumes on stop
* `-r | --reset-configuration`: resets the `config.yml` file from the `config.yml.example` file.
