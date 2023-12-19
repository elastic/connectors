# Full Elastic Stack with Connectors

We want to enable a user to get up and running with a full stack with connectors within 10 minutes. To help facilitate this, we want to provide an example configuration, Docker Compose configuration, and a bash script to startup and run a stack including Elasticsearch, Kibana, and Connectors. Light documentation should be provided, and full additional documentation will come in a future story.

A brand new user wants to try out Connectors and ingesting their data into an Elasticsearch instance before deciding if they want to purchase an Elastic license. To get them up and running, the user should be able to go to a tutorial that instructs them on how to download a package containing a template configuration file, a sample Docker configuration file and a script to run the stack. The documentation details out the steps for configuring various connectors and the options for running the Docker compose script. Upon setting up the configuration for their needs, the user can run the start script to light up localized Elasticsearch, Kibana, and Connectors containers with sensible defaults that they can now test out.

An existing Elastic user wants to incorporate Connectors into their workflow. The user may already have an existing self managed cluster set up and only needs to add a Connectors instance to their cluster. Reading the documentation, and looking at the sample configuration files for the Connectors container and the Docker compose, the user should be able to then configure and add a Connectors node to their cluster.

An existing Workplace Search user wants to start working with Connectors and working with Search Applications. Much like with the new user story above, having a robust set of documentation and an example configuration and script will help ease the transition into replacing Workplace Search connectors and searching across content with Connectors and Search Applications. As such, providing example configurations and a script to run will work as a first step for the user to orchestrate their application to suit their needs.

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

## Script command reference

### run-stack

```bash
$ ./scripts/stack/run-stack.sh
```

By default, the script will start an Elasticsearch and Kibana instance, then it will ask the user if they want
to configure the connectors, and finally start the connectors service.

Command line options:
* `-u | --update-images`: perform a fresh pull on the docker images
* `-n | --no-connectors`: do not run the connectors service or the connectors configuration
* `-x | --no-configuration`: do not ask to run the connectors configuration, but still run the service

### stop-stack

```bash
$ ./scripts/stack/stop-stack.sh
```

Command line options:
* `-v | --remove-volumes`: delete all data volumes on stop
