# Connectors CLI

Connectors CLI is a command-line interface to Elastic Connectors for use in your terminal or scripts.

**Warning:**
Connectors CLI is in tech preview.

- [Installation](#installation)
- [Configuration](#configuration)
- [Available commands](#available-commands)
- [Known bugs and limitations](#known-bugs-and-limitations)

## Installation
1. Clone the repository `git clone https://github.com/elastic/connectors.git`
2. Run `make clean install` to install dependencies and create executable files.
3. Connectors CLI is available via `./bin/connectors`

## Configuration
**Note:** Make sure your Elasticsearch instance is up and running.

1. Run `./bin/connectors login` to authenticate the CLI with an Elasticsearch instance.
2. Provide credentials
3. The command will create or ask to rewrite an existing configuration file in `./cli/config.yml`

By default, the CLI uses basic authentication method (username, password) however an API key can be used too. 
Run `./bin/connectors login --method apikey` to authenticate the CLI via your API key. 

When you run any command you can specify a configuration file using `-c` argument.
Example:

```bash
./bin/connectors -c <config-file-path.yml> connector list
```

## Available commands
### Getting help
Connectors CLI provides a `--help`/`-h` argument that can be used with any command to get more information.

For example:
```bash
./bin/connectors --help


Usage: connectors [OPTIONS] COMMAND [ARGS]...

Options:
  -v, --version          Show the version and exit.
  -c, --config FILENAME
  --help                 Show this message and exit.

Commands:
  connector  Connectors management
  index      Search indices management
  job        Sync jobs management
  login      Authenticate Connectors CLI with an Elasticsearch instance
```

### Commands

 - [`connectors connector create`](#connectors-connector-create)
 - [`connectors connector list`](#connectors-connector-list)
 - [`connectors job list`](#connectors-job-list)
 - [`connectors job cancel`](#connectors-job-cancel)
 - [`connectors job start`](#connectors-job-start)
 - [`connectors job view`](#connectors-job-view)

#### `connectors connector create`

Creates a new connector and links it to an Elasticsearch index. When executing the command you will be asked to provide a connector configuration based on the service type you selected. For instance, you will be asked for host, username, and password if you select `mysql`.

To bypass interactive mode you can use the `--from-file` argument, pointing to a key-value JSON file with a connectors configuration.

Examples:

```console
./bin/connectors connector create \
  --index-name my-index \
  --service-type sharepoint_online \
  --index-language en \
  --from-file sharepoint-config.json
```

This will create a new SharePoint Online connector with an Elasticsearch index `my-index` and configuration from `sharepoint-online-config.json`.

**Note**
See the connectors' [source code](../connectors/sources) to get more information about their configuration fields.

#### `connectors connector list`

Lists all the existing connectors

Examples:

```console
./bin/connectors connector list
```

This will display all existing connectors and the associated indices.

#### `connectors job list`
Lists all jobs and their stats.

Examples
```console
./bin/connectors job list -- <connector_id>
```

This will display all sync jobs including information like job status, number of indexed documents and index data volume associated with `connector_id`.

#### `connectors job cancel`
Marks the job as `cancelling` to let Connector services know that the job has to be canceled.

Examples:

```console
./bin/connectors job cancel -- <job_id>
```

#### `connectors job start`
Schedules a new sync job and lets Connector service pick it up.

Examples:

```console
./bin/connectors job start -- \
  -i <connector_id> \
  -t <job_type{full,incremental,access_control}> \
  -o <format{text,json}>
```

This will schedule a new sync job using job type and connector id. The output of the command contains a job id.

#### `connectors job view`
Shows information about a sync job.

Examples:

```console
./bin/connectors job view -- <job_id> -o <format{text,json}
```

This will display information about the job including job id, connector id, indexed document counts and index data value.

## Known bugs and limitations

Currently Connectors CLI does not support:

* Elasticsearch API keys for authentication
* all Kibana UI functionality
