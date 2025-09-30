_If looking for docker release instructions, please follow steps in https://github.com/elastic/connectors/blob/main/docs/RELEASING.md_

# Run Connector Service in Docker

To run the Connector Service in Docker, you need to have Docker installed locally.

This guide uses generally-available unix commands to demonstrate how to run the Connector Service in Docker.

Windows users might have to run them in [Unix Subsystem](https://learn.microsoft.com/en-us/windows/wsl/about), rewrite the commands in PowerShell, or execute them manually.

Please refer to the following Docker image registry to access and pull available versions of our service: [Elastic Connectors Docker Registry](https://www.docker.elastic.co/r/integrations/elastic-connectors).

Follow these steps:

- [Run Connector Service in Docker](#run-connector-service-in-docker)
  - [1. Create a Docker network.](#1-create-a-docker-network)
  - [2. Create a directory to be mounted into the Docker image.](#2-create-a-directory-to-be-mounted-into-the-docker-image)
  - [3. Download sample configuration file from this repository into newly created directory.](#3-download-sample-configuration-file-from-this-repository-into-newly-created-directory)
  - [4. Update the configuration file for your self-managed connector](#4-update-the-configuration-file-for-your-self-managed-connector)
  - [5. Run the Docker image.](#5-run-the-docker-image)

## 1. Create a Docker network.

```sh
docker network create elastic
```

## 2. Create a directory to be mounted into the Docker image.

This directory will contain the configuration file used to run the Connector Service. The examples in this guide will use the user's home directory (`~`).

```sh
cd ~ && mkdir connectors_service-config
```

## 3. Download sample configuration file from this repository into newly created directory.

You can download the file manually, or simply run the command below. Make sure to update the `--output` argument value if your directory name is different, or you want to use a different config file name.

```sh
curl https://raw.githubusercontent.com/elastic/connectors/main/config.yml.example --output ~/connectors_service-config/config.yml
```

## 4. Update the configuration file for your [self-managed connector](https://www.elastic.co/guide/en/elasticsearch/reference/current/es-build-connector.html#build-connector-usage)

If you're running the Connector Service against a dockerised version of Elasticsearch and Kibana, your config file will look like this:

```
# When connecting to your cloud deployment you should edit the host value
elasticsearch.host: http://host.docker.internal:9200
elasticsearch.api_key: <ELASTICSEARCH_API_KEY>

connectors:
  - connector_id: <CONNECTOR_ID_FROM_KIBANA>
    service_type: <SERVICE_TYPE_FROM_KIBANA>
    api_key: <API_KEY_FROM_KIBANA>

```

Using the `elasticsearch.api_key` is the recommended authentication method. However, you can also use `elasticsearch.username` and `elasticsearch.password` to authenticate with your Elasticsearch instance.

Note: You can change other default configurations by simply uncommenting specific settings in the configuration file and modifying their values.

After that, you can build your own Docker image to run:

```
docker build -t <TAG_OF_THE_IMAGE> .
```

For example, if you've created a custom version of MongoDB connector, you can tag it with the following command:

```
docker build -t connector/custom-mongodb:1.0 .
```

You can later use `<TAG_OF_THE_IMAGE>` instead of `docker.elastic.co/integrations/elastic-connectors:<VERSION>-SNAPSHOT` in the next step to run the Docker image.

If you're an Elastic employee, you may want to build a Chainguard-based image using `Dockerfile.wolfi`:

```
docker build -t <TAG_OF_THE_IMAGE> -f Dockerfile.wolfi .
```

## 5. Run the Docker image.

Now you can run the Docker image with the Connector Service. Here's an example command:

```sh
docker run \
-v ~/connectors_service-config:/config \
--network "elastic" \
--tty \
--rm \
docker.elastic.co/integrations/elastic-connectors_service:<VERSION>-SNAPSHOT \
/app/bin/elastic-ingest \
-c /config/config.yml
```

You might need to adjust some details here:

- `-v ~/connectors-config:/config \` - replace `~/connectors-config` with the directory that you've created in step 2 if you've chosen a different name for it.
- `--network "elastic"` - replace `elastic` with the network that you've created in step 1 if you've chosen a different name for it.
- `docker.elastic.co/integrations/elastic-connectors:<VERSION>-SNAPSHOT` - adjust the version for the connectors to match your Elasticsearch deployment version.
  - For Elasticsearch of version `<VERSION>` you can use `elastic-connectors:<VERSION>`for a stable revision of the connectors, or `elastic-connectors:<VERSION>-SNAPSHOT` if you want the latest nightly build of the connectors (not recommended).
  - If you are using nightly builds, you will need to run `docker pull docker.elastic.co/integrations/elastic-connectors:<VERSION>-SNAPSHOT` before starting the service. This ensures you're using the latest version of the Docker image.
- `-c /config/config.yml` - replace `config.yml` with the name of the config file you've put in the directory you've created in step 2.

> [!TIP]
> When starting a Docker container with the connector service against a local Elasticsearch cluster that has security and SSL enabled (like the [docker compose example](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html#docker-compose-file) from the Elasticsearch docs), it's crucial to handle the self-signed certificate correctly:
> 1. Ensure the Docker container running the connector service has the volume attached that contains the generated certificate. When using Docker Compose, Docker automatically adds a project-specific prefix to volume names based on the directory where your `docker-compose.yml` is located. When starting the connector service with `docker run`, use `-v <your_project>_certs:/usr/share/connectors/config/certs` to reference it. Replace `<your_project>` with the actual prefix, such as `elastic_docker_certs` if your `docker-compose.yml` that starts the Elasticsearch stack is in a directory named `elastic_docker`.
> 2. Make sure the connector service's `config.yml` correctly references the certificate with:
> ```
> elasticsearch.ca_certs: /usr/share/connectors/config/certs/ca/ca.crt
> ```
> 3. To avoid the certificate verification, configure `verify_certs` parameter which is `true` by default when SSL is enabled in connector service's `config.yml` as:
> ```
> elasticsearch.verify_certs: false
> ```
> Disclaimer: Setting `verify_certs` to `false` is not recommended in a production environment, as it may expose your application to security vulnerabilities.