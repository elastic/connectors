## Automated testing

### Installation

Automated testing CLI depends on `gcloud` and `vault` CLIs so you need to install them. 

#### Google cloud CLI
Follow the [offical installation](https://cloud.google.com/sdk/docs/install) guide. Once installed, run `gcloud auth login` and authorize using SSO. 

#### Vault
Download and install the Hashicorp Vault client version 1.14.1 or newer or via brew with `brew install vault`
1. `brew install vault`.
2. [Generate](https://github.com/elastic/infra/tree/master/docs/vault#github-auth) a github token to authorize in Vault.
3. Run `vault login --method github` to authorize your Vault cli.

#### Connector service
Run `make clean install` to generate executable files in `./bin` folder

### Usage

***Note:*** The working directory for `./bin/test-connectors` is `scripts/testing` so when you need to point to a file you should use it as a root directory. 
e.g. `./bin/test-connectors run-test my-testing-environment-name --es-version 8.12-SNAPSHOT --test-case scenarios/clients/spo_full_and_incremental_syncs.yml`

Run `./bin/test-connectors --help` or `./bin/test-connectors {command name} --help` to get more information about the cli. 

#### Running test with Elastic cloud deployment
If you want to run your test suite using a cloud Elasticsearch deployment follow the next steps: 
1. Create a cloud deployment
2. Download a credentials file (or create a new user)
3. Run `./bin/test-connectors run-test my-testing-environment-name --es-host {host} --es-username {user name} --es-password {password} --test-case {path to the test case file}`

#### Running test with local Elasticsearch
If you want to run your tests with local Elasticsearch you need to specify `--es-version` option. Like

`./bin/test-connectors run-test my-testing-environment-name --es-version 8.12-SNAPSHOT --test-case {path to the test case file}`

In this case, the cli will deploy an Elasticsearch instance in the same VM where the connector service will be running. 

#### Running test with a specific Connector service version

You can use any git reference such as commit sha, a tag, or a branch name. The cli will pull the defined git reference and run `make clean install`. 

Example: `./bin/test-connectors run-test my-testing-environment-name --es-version 8.12-SNAPSHOT --connectors-ref 8.12 --test-case {path to the test case file}`

#### Keeping your VM running when the tests passed
Sometimes it's useful to get access to the logs or make some changes in the code and run tests again. The CLI will print a list of useful commands you can use to access the VM resources like: 

`Access logs: gcloud compute ssh {my-testing-environment-name} --zone {VM zone} --command "tail -f ~/service.log"`

To automatically delete the VM you need to use `--delete` option. 

`./bin/test-connectors run-test my-testing-environment-name --es-version 8.12-SNAPSHOT --connectors-ref 8.12 --test-case {path to the test case file} --delete`

#### Using different machine type
All new VMs are based on a predefined image which is in turn based on `ubuntu-2204-lts` with python3 and docker installed. Custome images are not supported. You can change a machine type by providing `--vm-type`. Visit [the official GCP documentation](https://cloud.google.com/compute/docs/general-purpose-machines) to get more information. 

#### Adding a new test case

A test case should be present as a YAML file.

```YAML
---
scenarios:
  - index_name: search-demo-index-001
    service_type: sharepoint_online
    index_language: en
    connector_configuration: scenarios/clients/spo_sean_site.json
    native: false
    tests:
      - name: Full sync job is performed without errors
        job_type: full
        timeout: 60
        match: { status: 'completed'}
      - name: Incremental sync job is performed without errors
        job_type: incremental
        timeout: 20
        match: { status: 'completed'}
```

Each test case can contain multiple scenarios which will be executed one by one. Each scenario can have multiple test cases which will be executed one by one too. Currently, only one type of match is supported (job status). 

To add a new test case you need to create a new YAML file and describe your test case. Give it a thoughtful name. Also, you need to define `connector_configuration` that points to a connector configuration file.

Example: 

```JSON
{
  "tenant_id": "vault:path-to-spo-secret:TenantID",
  "tenant_name": "vault:path-to-spo-secret:Tenant Name",
  "use_text_extraction_service": false,
  "fetch_drive_item_permissions": true,
  "fetch_subsites": true,
  "client_id": "vault:path-to-spo-secret:ClientID",
  "fetch_unique_page_permissions": true,
  "secret_value": "vault:path-to-spo-secret:SecretValue",
  "enumerate_all_sites": false,
  "fetch_unique_list_item_permissions": true,
  "fetch_unique_list_permissions": true,
  "site_collections": "CustomWebSiteCollection",
  "use_document_level_security": false
}
```

Make sure that all the required fields are present in the secret. The CLI will get all the secrets and upload them to the VM.

Consider adding new secrets only for new connector sources. Reach out to the Search Productivity team if you need any help with Vault secrets. 

### Can I use gcloud?
Yes, it's already installed on your machine and the cli does not limit your use. Visit [the official documentation page](https://cloud.google.com/sdk/docs) for more information.

### Hygiene of use
Since the CLI creates virtual machines in GCP it's recommended to keep them running only when you need them. Let's save some trees 🌳🌳🌳. 
