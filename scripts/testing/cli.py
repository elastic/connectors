#!/usr/bin/python3

import json
import os
import re
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from subprocess import PIPE, STDOUT

import click
import yaml

__all__ = ["main"]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

VM_STARTUP_SCRIPT_PATH = f"startup-script={BASE_DIR}/startup_scipt.sh"
VM_INIT_ATTEMPTS = 30
SLEEP_TIMEOUT = 6
DOCKER_COMPOSE_FILE = f"{BASE_DIR}/docker-compose.yml"
PULL_CONNECTORS_SCRIPT = f"{BASE_DIR}/pull-connectors.sh"
CLI_CONFIG_FILE = "cli-config.yml"
CONNECTOR_SERVICE_CONFIG_FILE = "/var/app/config.yml"
VAULT_SECRETS_PREFIX = "secret/ent-search-team/"
ES_DEFAULT_HOST = "http://localhost:9200"
ES_DEFAULT_USERNAME = "elastic"
ES_DEFAULT_PASSWORD = "changeme"  # noqa: S105
SOURCE_MACHINE_IMAGE = "elastic-connectors-testing-base-image"
IMAGE_FAMILY = "ubuntu-2204-lts"

# VMs metadata
DIVISION = "engineering"
ORG = "entsearch"
TEAM = "ingestion"
PROJECT = "connectors-testing"


@click.group()
@click.pass_context
def cli(ctx):
    pass


@click.command(
    name="run-test",
    help="Spin up a VM, Elasticsearch and connectors services and run the tests",
)
@click.argument("name")
@click.option(
    "--vm-type",
    default="e2-highcpu-2",
    help="Virtual machine type. See more in https://cloud.google.com/compute/docs/general-purpose-machines",
)
@click.option(
    "--vm-zone",
    default="europe-west1-b",
    help="Virtual machine zone. See more in https://cloud.google.com/compute/docs/regions-zones",
)
@click.option(
    "--connectors-ref",
    default="main",
    help="A commit hash or a branch name of connectors repository",
)
@click.option(
    "--es-version",
    help="Elasticsearch version. If defined, the script will use a docker-compose file to start Elasticsearch",
)
@click.option("--es-host", help="Elasticsearch host", default=ES_DEFAULT_HOST)
@click.option(
    "--es-username", help="Elasticsearch username", default=ES_DEFAULT_USERNAME
)
@click.option(
    "--es-password", help="Elasticsearch password", default=ES_DEFAULT_PASSWORD
)
@click.option("--test-case", help="Test case file", type=click.Path(exists=True))
@click.option("--delete", is_flag=True, help="Deletes the VM once the tests passed")
@click.pass_context
def create_test_environment(
    ctx,
    name,
    vm_type,
    vm_zone,
    es_version,
    connectors_ref,
    es_host,
    es_username,
    es_password,
    test_case,
    delete,
):
    """
    Creates a new VM and runs the tests
    """

    # TODO: fail fast. Check all the configs/yaml files before creating a VM

    create_vm(name, vm_type, vm_zone)
    setup_stack(name, vm_zone, es_version, connectors_ref, es_host)
    run_scenarios(name, es_host, es_username, es_password, vm_zone, test_case)

    if delete is True:
        ctx.invoke(delete_test_environment, name=name, vm_zone=vm_zone)
    else:
        print_help(name, vm_zone)


cli.add_command(create_test_environment)


@click.command(name="delete", help="Deletes a VM")
@click.argument("name")
@click.option("--vm-zone", default="europe-west1-b")
def delete_test_environment(name, vm_zone):
    """
    Deletes the VM
    """
    cmd = [
        "gcloud",
        "compute",
        "instances",
        "delete",
        name,
        "--quiet",
        "--zone",
        vm_zone,
    ]
    click.echo("Deleting the VM")
    run_gcloud_cmd(cmd)

    click.echo("The VM has been deleted")


cli.add_command(delete_test_environment)


def print_help(name, vm_zone):
    """
    Prints a list of commands that can be used to interact with the setup
    """
    logs_cmd = " ".join(
        [
            "gcloud",
            "compute",
            "ssh",
            name,
            "--zone",
            vm_zone,
            "--command",
            '"tail -f ~/service.log"',
        ]
    )
    connectors_cmd = " ".join(
        [
            "gcloud",
            "compute",
            "ssh",
            name,
            "--zone",
            vm_zone,
            "--command",
            f'"/var/app/bin/connectors -c ~/{CLI_CONFIG_FILE} connector list"',
        ]
    )
    sync_jobs_cmd = " ".join(
        [
            "gcloud",
            "compute",
            "ssh",
            name,
            "--zone",
            vm_zone,
            "--command",
            f'"/var/app/bin/connectors -c ~/{CLI_CONFIG_FILE} job list CONNECTOR_ID"',
        ]
    )

    click.echo("You can use the following commands to interact with the setup:")
    click.echo("Access logs: " + click.style(logs_cmd, fg="green"))
    click.echo("List of connectors: " + click.style(connectors_cmd, fg="green"))
    click.echo("List of sync jobs: " + click.style(sync_jobs_cmd, fg="green"))


def create_vm(name, vm_type, vm_zone):
    """
    Creates a new VM and waits until the startup script finishes its work
    """

    with click.progressbar(label="Creating a new VM...", length=100) as steps:
        cmd = [
            "gcloud",
            "compute",
            "instances",
            "create",
            name,
            "--async",
            "--source-machine-image",
            SOURCE_MACHINE_IMAGE,
            "--image-family",
            IMAGE_FAMILY,
            "--machine-type",
            vm_type,
            "--zone",
            vm_zone,
            f"--metadata-from-file={VM_STARTUP_SCRIPT_PATH}",
        ]
        result = run_gcloud_cmd(cmd)

        steps.update(25)

        attempts = 0
        while True:
            if attempts >= VM_INIT_ATTEMPTS:
                click.echo("Timeout!")
                raise click.Abort()
            attempts = attempts + 1

            # Tries to ssh to the VM and check if the startup script finished its work
            cmd = [
                "gcloud",
                "compute",
                "ssh",
                name,
                "--zone",
                vm_zone,
                "--ssh-flag=",
                "-q",
                "--command",
                "sudo ls /var/log/startup-is-finished ",
            ]
            result = subprocess.run(cmd, stdout=PIPE, stderr=STDOUT)  # noqa: S603

            stdout = result.stdout.decode("utf-8")
            # indicates that the VM is booting
            if re.search("Connection refused", stdout) or re.search(
                "SSH connectivity issues", stdout
            ):
                steps.update(25)
            # indicates that the VM is ready but the startup script
            elif result.returncode == 1 or result.returncode == 2:
                pass
            elif result.returncode == 0:
                steps.update(25)
                break
            else:
                click.echo(stdout)
                raise click.Abort()

            time.sleep(SLEEP_TIMEOUT)

        # update the VM metadata
        cmd = [
            "gcloud",
            "compute",
            "instances",
            "add-metadata",
            name,
            "--zone",
            vm_zone,
            "--metadata",
            f"division={DIVISION},org={ORG},team={TEAM},project={PROJECT}",
        ]

        run_gcloud_cmd(cmd)

        steps.update(25)


def render_connector_configuration(file_path):
    """
    Reads the connector configuration file and replaces all the values that start with `vault:`
    with the values from Vault
    """
    configuration = {}
    with open(os.path.join(BASE_DIR, file_path), "r") as f:
        configuration = json.loads(f.read())
        for key, item in configuration.items():
            if type(item) is str and item.startswith("vault:"):
                configuration[key] = read_from_vault(item)

    return configuration


def setup_stack(name, vm_zone, es_version, connectors_ref, es_host):
    with click.progressbar(label="Setting up the stack...", length=100) as steps:
        # Upload pull-connectors file
        cmd = [
            "gcloud",
            "compute",
            "scp",
            PULL_CONNECTORS_SCRIPT,
            f"{name}:~/",
            "--zone",
            vm_zone,
        ]
        run_gcloud_cmd(cmd)
        steps.update(1)

        # pull connectors repo
        cmd = [
            "gcloud",
            "compute",
            "ssh",
            name,
            "--zone",
            vm_zone,
            "--command",
            f"sudo ./{os.path.basename(PULL_CONNECTORS_SCRIPT)} {connectors_ref}",
        ]
        run_gcloud_cmd(cmd)
        steps.update(9)

        # check if it's a cloud/serverless deployment
        if es_host != ES_DEFAULT_HOST:
            steps.update(90)
            return

        # upload Elasticsearch docker compose file
        cmd = [
            "gcloud",
            "compute",
            "scp",
            DOCKER_COMPOSE_FILE,
            f"{name}:~/",
            "--zone",
            vm_zone,
        ]
        run_gcloud_cmd(cmd)

        steps.update(1)

        # run docker compose file
        cmd = [
            "gcloud",
            "compute",
            "ssh",
            name,
            "--zone",
            vm_zone,
            "--command",
            f"sudo ES_VERSION={es_version} docker compose -f ~/{os.path.basename(DOCKER_COMPOSE_FILE)} up -d",
        ]
        run_gcloud_cmd(cmd)

        steps.update(9)

        # wait when Elasticsearch starts
        for _i in range(1, 16):
            time.sleep(SLEEP_TIMEOUT)
            steps.update(4)

        steps.update(20)


# TODO: Use the same config file for the CLI and the connector service
@contextmanager
def cli_config(es_host, es_username, es_password):
    """
    Creates a temporary file with the CLI configuration and deletes it after the context is closed
    """
    try:
        config = {
            "elasticsearch": {
                "host": es_host,
                "username": es_username,
                "password": es_password,
            }
        }

        file_name = None

        with tempfile.NamedTemporaryFile(delete=False, mode="w") as fp:
            file_name = fp.name
            fp.write(yaml.dump(config))
            fp.close()

        yield file_name
    finally:
        if os.path.isfile(file_name):
            os.remove(file_name)


@contextmanager
def connector_service_config(es_host, es_username, es_password):
    """
    Creates a temporary file with the connector service configuration and deletes it after the context is closed
    """
    try:
        config = {
            "elasticsearch.host": es_host,
            "elasticsearch.username": es_username,
            "elasticsearch.password": es_password,
        }

        file_name = None

        with tempfile.NamedTemporaryFile(delete=False, mode="w") as fp:
            file_name = fp.name
            fp.write(yaml.dump(config))
            fp.close()

        yield file_name
    finally:
        if os.path.isfile(file_name):
            os.remove(file_name)


def run_scenarios(name, es_host, es_username, es_password, vm_zone, test_case):
    """
    Runs the scenarios from the test case file
    """
    scenarios = yaml.safe_load(open(test_case))

    with cli_config(es_host, es_username, es_password) as cli_config_file:
        cmd = [
            "gcloud",
            "compute",
            "scp",
            cli_config_file,
            f"{name}:~/{CLI_CONFIG_FILE}",
            "--zone",
            vm_zone,
        ]

        run_gcloud_cmd(cmd)

    cmd = [
        "gcloud",
        "compute",
        "ssh",
        name,
        "--zone",
        vm_zone,
        "--command",
        f"sudo /var/app/bin/connectors -c {CLI_CONFIG_FILE} connector list",
    ]
    run_gcloud_cmd(cmd)

    with connector_service_config(
        es_host, es_username, es_password
    ) as service_config_file:
        cmd = [
            "gcloud",
            "compute",
            "scp",
            service_config_file,
            f"{name}:{CONNECTOR_SERVICE_CONFIG_FILE}",
            "--zone",
            vm_zone,
        ]
        run_gcloud_cmd(cmd)

    for scenario in scenarios["scenarios"]:
        try:
            file_name = None
            rendered_connector_configuration = render_connector_configuration(
                scenario["connector_configuration"]
            )
            with tempfile.NamedTemporaryFile(delete=False, mode="w") as fp:
                file_name = fp.name
                fp.write(json.dumps(rendered_connector_configuration))
                fp.close()

            cmd = [
                "gcloud",
                "compute",
                "scp",
                fp.name,
                f"{name}:~/{os.path.basename(scenario['connector_configuration'])}",
                "--zone",
                vm_zone,
            ]

            run_gcloud_cmd(cmd)
        finally:
            if file_name is not None and os.path.isfile(file_name):
                os.remove(file_name)

        # prepare connector
        native = "--native" if scenario["native"] else ""
        connector_command = [
            "connector",
            "create",
            "--index-name",
            scenario["index_name"],
            "--service-type",
            scenario["service_type"],
            "--index-language",
            scenario["index_language"],
            native,
            "--from-file",
            os.path.basename(scenario["connector_configuration"]),
            "--update-config",
            "--connector-service-config",
            CONNECTOR_SERVICE_CONFIG_FILE,
        ]
        cmd = [
            "gcloud",
            "compute",
            "ssh",
            name,
            "--zone",
            vm_zone,
            "--command",
            f"sudo /var/app/bin/connectors -c {CLI_CONFIG_FILE} {' '.join(connector_command)}",
        ]

        result = run_gcloud_cmd(cmd)
        connector_id = re.search("ID:\s([\w\-_]+)", result.stdout.decode("utf-8"))[1]

        # Start the service
        cmd = [
            "gcloud",
            "compute",
            "ssh",
            name,
            "--zone",
            vm_zone,
            "--command",
            f"sudo /var/app/bin/elastic-ingest -c {CONNECTOR_SERVICE_CONFIG_FILE} --debug --filebeat >~/service.log 2>&1 &",
        ]
        run_gcloud_cmd(cmd)

        for test in scenario["tests"]:
            click.echo(f"Run test: {test['name']}")
            cmd = [
                "gcloud",
                "compute",
                "ssh",
                name,
                "--zone",
                vm_zone,
                "--command",
                f"sudo /var/app/bin/connectors -c {CLI_CONFIG_FILE} job start -i {connector_id} -t {test['job_type']} --format json",
            ]
            result = run_gcloud_cmd(cmd)

            job_id = json.loads(result.stdout.decode("utf-8"))["id"]

            timeout = 0
            timeout_step = test["timeout"] / 5
            while True:
                cmd = [
                    "gcloud",
                    "compute",
                    "ssh",
                    name,
                    "--zone",
                    vm_zone,
                    "--command",
                    f"sudo /var/app/bin/connectors -c {CLI_CONFIG_FILE} job view {job_id} --format json",
                ]
                result = run_gcloud_cmd(cmd)
                job = json.loads(result.stdout.decode("utf-8"))

                if job["job_status"] != test["match"]["status"]:
                    time.sleep(timeout_step)
                    timeout += timeout_step
                else:
                    click.echo(click.style(f"Test {test['name']} passed", fg="green"))
                    break

                if timeout >= test["timeout"]:
                    click.echo(click.style(f"Test {test['name']} failed", fg="red"))
                    break


# TODO Read all the fields in one call
def read_from_vault(key):
    """
    Reads a secret from Vault and returns its value
    """
    _, secret_prefix, field = key.split(":")
    cmd = ["vault", "read", "-field", field, f"{VAULT_SECRETS_PREFIX}/{secret_prefix}"]
    result = subprocess.run(cmd, stdout=PIPE, stderr=STDOUT)  # noqa: S603
    if result.returncode != 0:
        click.echo(result.stdout, err=True)
        raise click.Abort(result.stdout)

    return result.stdout.decode("utf-8")


def run_gcloud_cmd(cmd):
    """
    Runs a gcloud command and raises an exception if the command failed
    """
    result = subprocess.run(cmd, stdout=PIPE, stderr=STDOUT)  # noqa: S603
    if result.returncode != 0:
        click.echo(result.stdout, err=True)
        raise click.Abort(result.stdout)

    return result


def main(args=None):
    cli()


if __name__ == "__main__":
    main()
