Connectors CLI is a command-line interface to Elastic Connectors for use in your terminal or your scripts. 

**Warning:**
Connectors CLI in tech preview.

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

When you run any command you can specify a configuration file using `-c` argument. 
Example: 

```bash
./bin/connectors -c <config-file-path.yml> connector list
```

## Available commands
### Getting help
Connectors CLI provides `--help` argument that can be used with any command to get more information. 

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

### Commands list

 - [connectors connector create](#connectors-connector-create)
 - [connectors connector list](#connectors-connector-list)
 - [connectors job list](#connectors-job-list)
 - [connectors job cancel](#connectors-job-cancel)
 - [connectors job start](#connectors-job-start)
 - [connectors job view](#connectors-job-view)
 
 
#### connectors connector create
Creates a new connector and links it to an Elasticsearch index. When executing the command you will be asked to provide a connector configuration based on the service type you selected. For instance, you will be asked for host, username, and password if you select `mysql`. 

To bypass interactive mode you can pass `--from-file` argument pointing to a key-value JSON file with connectors configuration.

Examples:

```bash
./bin/connectors connector create --index-name my-index --service-type sharepoint_online --index-language en --from-file sharepoint-config.json
```
This will create a new Sharepoint Online connector with an Elasticsearch index `my-index` and configuration from `sharepoint-online-config.json`. 

**Note**
See the connectors' [source code](../connectors/sources) to get more information about their configuration fields.

#### connectors connector list

Lists all the existing connectors

Examples: 

```bash
./bin/connectors connector list
```

It will display all existing connectors and the associated indices.

#### connectors job list
Lists all jobs and their stats. 

Examples
```bash
./bin/connectors job list -- <connector_id>
```

It will display all sync jobs including information like job status, number of indexed documents and index data volume associated with `connector_id`. 

#### connectors job cancel
Marks the job as `cancelling` to let Connector services know that the job has to be canceled. 

Examples: 

```bash
./bin/connectors job cancel -- <job_id>
```

#### connectors job start
Schedules a new sync job and lets Connector service pick it up. 

Examples: 

```bash
./bin/connectors job start -- -i <connector_id> -t <job_type{full,incremental,access_control}> -o <format{text,json}>
```

It will schedule a new sync job using job type and connector id. The outcome of the command contains a job id.

#### connectors job view
Shows information about a sync job. 

Examples: 

```bash
./bin/connectors job view -- <job_id> -o <format{text,json}
```

It will display information about the job including job id, connector id, indexed document counts and index data value. 

## Known bugs and limitations
1. Does not support Elasticsearch API keys for authentication.
2. Does not support all Kibana UI functionality.
