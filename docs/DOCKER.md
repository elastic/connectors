# Run Connector Service in Docker

To run the Connector Service in Docker, you need to have Docker installed locally.

This guide uses generally-available unix commands to demonstrate how to run the Connector Service in Docker.

Windows users might have to run them in [Unix Subsystem](https://learn.microsoft.com/en-us/windows/wsl/about), rewrite the commands in PowerShell, or execute them manually.

Follow these steps:

1. [Create network](#1-create-a-docker-network)
2. [Create directory](#2-create-a-directory-to-be-mounted-into-the-docker-image)
3. [Download config file](#3-download-sample-configuration-file-from-this-repository-into-newly-created-directory)
4. [Update config file](#4-update-the-configuration-file-for-your-on-prem-connectorhttpswwwelasticcoguideenenterprise-searchcurrentbuild-connectorhtmlbuild-connector-usage)
5. [Run the docker image](#5-run-the-docker-image)

## 1. Create a Docker network.

```sh
docker network create elastic
```

## 2. Create a directory to be mounted into the Docker image.

This directory will contain the configuration file used to run the Connector Service. The examples in this guide will use the user's home directory (`~`).

```sh
cd ~ && mkdir connectors-python-config
```

## 3. Download sample configuration file from this repository into newly created directory.

You can download the file manually, or simply run the command below. Make sure to update the `--output` argument value if your directory name is different, or you want to use a different config file name.

```sh
curl https://raw.githubusercontent.com/elastic/connectors-python/main/config.yml --output ~/connectors-python-config/config.yml
```

## 4. Update the configuration file for your [on-prem connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html#build-connector-usage)

If you're running the Connector Service against a dockerised version of Elasticsearch and Kibana, your config file will look like this:

```
# When connecting to your cloud deployment you should edit this value
elasticsearch.host: http://host.docker.internal:9200

# Using `api_key` is recommended instead of `username`/`password`
elasticsearch.api_key: <ELASTICSEARCH_API_KEY>
# elasticsearch.username: elastic
# elasticsearch.password: <YOUR_PASSWORD>

connectors:
  - connector_id: <CONNECTOR_ID_FROM_KIBANA>
    service_type: <SERVICE_TYPE_FROM_KIBANA>
    api_key: <API_KEY_FROM_KIBANA>

```

to make Connectors Service run properly for you.

After that, you can build your own Docker image to run:

```
docker build -t <TAG_OF_THE_IMAGE> .
```

For example, if you've created a custom version of MongoDB connector, you can tag it with the following command:

```
docker build -t connector/custom-mongodb:1.0 .
```

You can later use `<TAG_OF_THE_IMAGE>` instead of `docker.elastic.co/enterprise-search/elastic-connectors:8.10.0.0-SNAPSHOT` in the next step to run the Docker image.

## 5. Run the Docker image.

Now you can run the Docker image with the Connector Service. Here's an example command:

```sh
docker run \
-v ~/connectors-python-config:/config \
--network "elastic" \
--tty \
--rm \
docker.elastic.co/enterprise-search/elastic-connectors:8.10.0.0-SNAPSHOT \
/app/bin/elastic-ingest \
-c /config/config.yml
```

You might need to adjust some details here:

- `-v ~/connectors-python-config:/config \` - replace `~/connectors-python-config` with the directory that you've created in step 2 if you've chosen a different name for it.
- `--network "elastic"` - replace `elastic` with the network that you've created in step 1 if you've chosen a different name for it.
- `docker.elastic.co/enterprise-search/elastic-connectors:8.10.0.0-SNAPSHOT` - adjust the version for the connectors to match your Elasticsearch deployment version.
  - For Elasticsearch of version 8.7 you can use `elastic-connectors:8.10.0.0`for a stable revision of the connectors, or `elastic-connectors:8.10.0.0-SNAPSHOT` if you want the latest nightly build of the connectors (not recommended).
  - If you are using nightly builds, you will need to run `docker pull docker.elastic.co/enterprise-search/elastic-connectors:8.10.0.0-SNAPSHOT` before starting the service. This ensures you're using the latest version of the Docker image.
- `-c /config/config.yml` - replace `config.yml` with the name of the config file you've put in the directory you've created in step 2.
