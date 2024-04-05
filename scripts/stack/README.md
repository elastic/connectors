# Full Elastic Stack with Connectors

We know that setting up the Connectors service may look a bit daunting, so to help with this, we pulled together a set of scripts to enable a user to get up and running quickly. Here, we provide an example Connectors configuration, a Docker Compose file, and a bash script to run a stack comprising of Elasticsearch, Kibana, and Connectors.
Once running, you can start to ingest data into an Elasticsearch instance via Elastic Connectors.
These scripts can also be used as a starting place for adding Connectors to your production environment.

# Contents

* [Prerequisites](#prerequisites)
* [Running the Stack](#running-the-stack)
* [Prompted Configuration](#prompted-configuration)
* [Manual Configuration](#manual-configuration)
* [Script command reference](#script-command-reference)
  * [run-stack.sh](#run-stacksh)
  * [stop-stack.sh](#stop-stacksh)
  * [view-connectors-logs.sh](#view-connectors-logssh)

## Prerequisites
* Linux or macOS (Although Windows can run the Connectors service, it is not currently supported via these scripts)
* Python 3.10
* Docker with Docker Compose Installed
* It is recommended to run Docker with at least 4GB of available RAM.

## Running the Stack

The scripts use a default password for the `elastic` user, however, if you wish to use a different password, you can specify the environment variable `ELASTIC_PASSWORD` before running the script. E.g.:

```bash
ELASTIC_PASSWORD="my_new_password" ./scripts/stack/run-stack.sh
```

Note that this method of setting the password, or using the default password, is not secure and should not be used in a production environment without proper steps taken to ensure the password is secured.
This can include editing the `docker-compose.yml` file and the `set-env.sh` script to hardcode a different password amongst other ways.

Alternatively, if you do not wish to use a different set of credentials, or an API key for authentication, you can run the `./copy-config.sh` script to create a
copy of the default [config.yml](../../config.yml) file, and edit the resulting file that will be created in the `scripts/stack/connectors-config` folder.

By default, these Connector scripts will use a `SNAPSHOT` version of the Docker image.
The version is defined in the [VERSION file](../../connectors/VERSION).
If you do not wish to use a `SNAPSHOT` version, add the `--no-snapshot` flag when you run the script.
You can also specify an alternative base version by setting the environment variable `CONNECTORS_VERSION` before running the script. e.g.:

```bash
CONNECTORS_VERSION=8.11.2.0 ./scripts/stack/run-stack.sh --no-snapshot
```

When running the script, the Elasticsearch and Kibana versions will use the same stack version as the `CONNECTORS_VERSION`, however, you can additionally specify these before running:

```bash
ELASTICSEARCH_VERSION=8.11.2 KIBANA_VERSION=8.11.2 CONNECTORS_VERSION=8.11.2.0 ./scripts/stack/run-stack.sh
```

Once the stack is running, you can monitor the logs from the Connectors instance by running:
```bash
./scripts/stack/view-connectors-logs.sh
```
or:
```bash
docker-compose -f ./scripts/stack/docker/docker-compose.yml logs -f elastic-connectors
```

## Prompted Configuration

If you run the `run-stack.sh` command without any flags, by default the script will
ask if you want to set up the connectors configuration. If you enter "y" to run
the configurator, it will take you through a set of prompts using the
[Connectors CLI](../../connectors/connectors_cli.py) to create a new index and connector.

The resulting configuration will be saved to the [scripts/stack/connectors-config/config.yml](./connectors-config/config.yml)
file that is used to start the Connectors container. Note that using the configurator,
it will modify the [connectors config.yml](./connectors-config/config.yml) file. You can restore it
by copying over the [example config.yml.example](./connectors-config/config.yml.example) file
to `config.yml` in the directory.

If you configured a new connector, you will need to go into Kibana to start or schedule a sync.
Running the scripts, you can log in at http://localhost:5601/ with the username `elastic` and your password (`changeme` is the default, unless you specified a different `ELASTIC_PASSWORD` earlier).
Then, to complete this in Kibana:
* From the main menu, choose "Content" under the "Search" heading
* Click on your index name
* Click on the "Configuration" tab
  * Note, the API key will be already pre-configured, and you do not need to generate a new one
  * Optionally, change the name or add a description
  * Ensure your connector settings are the way you want, if not edit them to their correct values
  * Finally, set your schedule and sync

Note - some of the connectors require a valid Elasticsearch license above `basic`.
To see which connectors require a higher license tier, see the [Connectors reference](https://www.elastic.co/guide/en/enterprise-search/current/connectors-references.html).

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
    8. Copy the resulting connector_id, service_type, and api_key into a new entry in the `./scripts/stack/connectors-config/config.yml` file
    9. Repeat as necessary for all the connectors you wish too create
3. run `./scripts/stack/run-stack.sh --no-configuration --connectors-only` again to run the Connectors service without prompting for configuration. Once running:
    1. From the main menu, choose "Content" under the "Search" heading
    2. Click on your index name
    3. Click on the "Configuration" tab
    4. Complete the configuration and optionally start your sync
    5. Repeat these steps for all the connectors you created

## Script command reference

### run-stack.sh

This is the main script that sets up and runs an Elasticsearch, Kibana, and Connectors stack.

```bash
./scripts/stack/run-stack.sh
```

By default, the script will start an Elasticsearch and Kibana instance, then it will ask the user if they want
to configure the connectors, and finally start the connectors service.

Command line options:
* `-u | --update-images`: perform a fresh pull on the docker images
* `-n | --no-connectors`: do not run the connectors service or the connectors configuration
* `-x | --no-configuration`: do not ask to run the connectors configuration, but still run the service
* `-c | --connectors-only`: only start the connectors service. Useful if Elasticsearch and Kibana are already running
* `-s | --no-snapshot`: by default, the scripts will use `SNAPSHOT` versions of the Docker images. Specify this to not use a SNAPSHOT.


### stop-stack.sh

This script is used to stop the stack, and optionally remove any data volumes and reset the configuration file.

```bash
./scripts/stack/stop-stack.sh
```

Command line options:
* `-v | --remove-volumes`: delete all data volumes on stop
* `-r | --reset-configuration`: removes the current configuration

### view-connectors-logs.sh

Views the Connectors Docker container logs, optionally continously watching the log output.

```bash
./scripts/stack/view-connectors-logs.sh
```

Command line options:
* `-w | --watch-logs`: watching the log output. Without this flag, only up to the last 20 log lines will be shown.
