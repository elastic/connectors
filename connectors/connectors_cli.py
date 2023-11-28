#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Command Line Interface.

This is the main entry point of the framework. When the project is installed as
a Python package, an `elastic-ingest` executable is added in the PATH and
executes the `main` function of this module, which starts the service.
"""
import asyncio
import click
import yaml
from connectors.cli.auth import Auth, CONFIG_FILE_PATH
from connectors.cli.index import Index
from connectors.cli.job import Job
from connectors import __version__  # NOQA
from connectors.cli.connector import Connector
from tabulate import tabulate
import os

SERVICE_TYPES = {
    'mongodb': 'connectors.sources.mongo:MongoDataSource',
    's3': 'connectors.sources.s3:S3DataSource',
    'dir': 'connectors.sources.directory:DirectoryDataSource',
    'mysql': 'connectors.sources.mysql:MySqlDataSource',
    'network_drive': 'connectors.sources.network_drive:NASDataSource',
    'google_cloud_storage': 'connectors.sources.google_cloud_storage:GoogleCloudStorageDataSource',
    'azure_blob_storage': 'connectors.sources.azure_blob_storage:AzureBlobStorageDataSource',
    'postgresql': 'connectors.sources.postgresql:PostgreSQLDataSource',
    'oracle': 'connectors.sources.oracle:OracleDataSource',
    'sharepoint_server': 'connectors.sources.sharepoint_server:SharepointServerDataSource',
    'mssql': 'connectors.sources.mssql:MSSQLDataSource',
    'jira': 'connectors.sources.jira:JiraDataSource',
    'confluence': 'connectors.sources.confluence:ConfluenceDataSource',
    'dropbox': 'connectors.sources.dropbox:DropboxDataSource',
    'servicenow': 'connectors.sources.servicenow:ServiceNowDataSource',
    'sharepoint_online': 'connectors.sources.sharepoint_online:SharepointOnlineDataSource',
    'github': 'connectors.sources.github:GitHubDataSource'
}

__all__ = ["main"]

# Main group
def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(__version__)

# @TODO print help page when no arguments passed
@click.group(invoke_without_command=True)
@click.option('-v', '--version', is_flag=True, callback=print_version,
              expose_value=False, is_eager=True)
@click.option('-c', '--config', type=click.File('rb'))
@click.pass_context
def cli(ctx, config):
    ctx.ensure_object(dict)
    if config:
        ctx.obj['config'] = yaml.safe_load(config)
    elif os.path.isfile(CONFIG_FILE_PATH):
        with open(CONFIG_FILE_PATH, "r") as f:
            ctx.obj['config'] = yaml.safe_load(f.read())
    else:
        # todo raise an exception
        pass

    # ctx = click.get_current_context()
    # click.echo(ctx.get_help())
    # ctx.exit()

@click.command(help="Authenticate Connectors CLI with an Elasticsearch instance")
@click.option('--host', prompt="Elastic host")
@click.option('--username', prompt="Username")
@click.option('--password', prompt="Password", hide_input=True)

def login(host, username, password):
    auth = Auth(host, username, password)
    if auth.is_config_present():
        click.confirm(click.style('Config is already present. Are you sure you want to override it?😱', fg='yellow'), abort=True)
    if auth.authenticate():
        click.echo(click.style("Authentication successful. You're breathtaking.", fg='green'))
    else:
        click.echo('')
        click.echo(click.style("Authentication failed. Please check your credentials.", fg='red'), err=True)
    return



cli.add_command(login)

# Connector group
@click.group(invoke_without_command=True)
@click.pass_obj
def connector(obj):
    pass

@click.command(help="List all existing connectors")
@click.pass_obj
def list(obj):
    click.echo("")
    connector = Connector(config=obj['config']['elasticsearch'])
    coro = connector.list_connectors()

    try:
        connectors = asyncio.get_event_loop().run_until_complete(coro)
        if len(connectors) == 0:
            click.echo("No connectors found")

        click.echo(f"Showing {len(connectors)} connectors \n")

        table_rows = []
        for connector in connectors:

            formatted_connector = [
                click.style(connector.id, blink=True, fg="green"),
                click.style(connector.index_name, blink=True, fg="white"),
                click.style(connector.service_type, blink=True, fg='white'),
                click.style(connector.status.value, fg="white"),
                click.style(connector.last_sync_status.value, fg="white")
            ]
            table_rows.append(formatted_connector)

        click.echo(tabulate(table_rows, headers=["ID", "Index name", "Service type", "Status", "Last sync job status"]))
    except asyncio.CancelledError as e:
        click.echo(e)

# TODO add language prompt
@click.command(help="Creates a new connector and a search index")
@click.option('--index_name', prompt=f"{click.style('?', blink=True, fg='green')} Search index name (search-)")
@click.option('--service_type', prompt=f"{click.style('?', blink=True, fg='green')} Service type", type=click.Choice(SERVICE_TYPES.keys(), case_sensitive=False))
@click.pass_obj
def create(obj, index_name, service_type):
    index_name = f"search-{index_name}"
    connector = Connector(obj['config']['elasticsearch'])
    configuration = connector.service_type_configuration(source_class=SERVICE_TYPES[service_type])

    prompt  = lambda : click.prompt(f"{click.style('?', blink=True, fg='green')} {item['label']}", default=item.get('value', None), hide_input=(True if item.get('sensitive') == True else False))

    # first fill in the fields that do not depend on other fields
    for key, item in configuration.items():
        if 'depends_on' in item:
            continue

        configuration[key]['value'] = prompt()

    for key, item in configuration.items():
        if not 'depends_on' in item:
            continue

        if all(configuration[field_item['field']]['value'] == field_item['value'] for field_item in item['depends_on']):
            configuration[key]['value'] = prompt()

    result = connector.create(index_name, service_type, configuration)
    click.echo("Connector (ID: " + click.style(result[1], fg="green") +  ", service_type: " + click.style(service_type, fg='green') + ") has been created!")

connector.add_command(create)
connector.add_command(list)

cli.add_command(connector)

# Index group
@click.group(invoke_without_command=True)
@click.pass_obj
def index(obj):
    pass

@click.command(help="Show all indices")
@click.pass_obj
def list(obj):
    click.echo("")
    index = Index(config=obj['config']['elasticsearch'])
    indices = index.list_indices()

    if len(indices) == 0:
        click.echo("No indices found")

    click.echo(f"Showing {len(indices)} indices \n")
    table_rows = []
    for index in indices:
        formatted_index = [
            click.style(index, blink=True, fg="white"),
            click.style(indices[index]['primaries']['docs']['count'])
        ]
        table_rows.append(formatted_index)

    click.echo(tabulate(table_rows, headers=["Index name", "Number of documents"]))


index.add_command(list)

@click.command(help="Remove all documents from the index")
@click.pass_obj
@click.argument('index', nargs=1)
def clean(obj, index):
    index_cli = Index(config=obj['config']['elasticsearch'])
    click.confirm(click.style('Are you sure you want to clean ' + index + '?😱', fg='yellow'), abort=True)
    if index_cli.clean(index):
        click.echo(click.style("The index has been cleaned. You're awesome.", fg='green'))
    else:
        click.echo('')
        click.echo(click.style("Something went wrong. Please try again later or check your credentials", fg='red'), err=True)

index.add_command(clean)

@click.command(help="Delete an index")
@click.pass_obj
@click.argument('index', nargs=1)
def delete(obj, index):
    index_cli = Index(config=obj['config']['elasticsearch'])
    click.confirm(click.style('Are you sure you want to delete ' + index + '?😱', fg='yellow'), abort=True)
    if index_cli.delete(index):
        click.echo(click.style("The index has been deleted. You're amazing.", fg='green'))
    else:
        click.echo('')
        click.echo(click.style("Something went wrong. Please try again later or check your credentials", fg='red'), err=True)

index.add_command(delete)

cli.add_command(index)

# Job group
@click.group(invoke_without_command=True)
@click.pass_obj
def job(obj):
    pass


@click.command(help="Start a job. -i to pass connector id, -t to pass job type")
@click.pass_obj
@click.option('-i')
@click.option('-t')
def start(obj, i, t):
    job_cli = Job(config=obj['config']['elasticsearch']) 
    click.echo('Starting a job...')
    if job_cli.start(connector_id=i, job_type=t):
        click.echo(click.style("The job has been started. You're phenomenous!", fg='green'))
    else:
        click.echo('')
        click.echo(click.style("Something went wrong. Please try again later or check your credentials", fg='red'), err=True)

job.add_command(start)

@click.command(help="List of jobs sorted by date. -i to pass connector name, -n to pass index name, -j to pass job id")
@click.pass_obj
@click.option('-i')
@click.option('-n')
@click.option('-j')
def list(obj, i, n, j):
    job_cli = Job(config=obj['config']['elasticsearch']) 
    jobs = job_cli.list_jobs(connector_id=i, index_name=n, job_id=j)

    if len(jobs) == 0:
        click.echo("No jobs found")

    click.echo(f"Showing {len(jobs)} jobs \n")
    table_rows = []
    for job in jobs:
        formatted_job = [
            click.style(job.id, blink=True, fg="green"),
            click.style(job.connector_id, blink=True, fg="white"),
            click.style(job.index_name, blink=True, fg="white"),
            click.style(job.status.value, blink=True, fg="white"),
            click.style(job.job_type.value, blink=True, fg="white"),
            click.style(job.indexed_document_count, blink=True, fg="white"),
            click.style(job.indexed_document_volume, blink=True, fg="white"),
            click.style(job.deleted_document_count, blink=True, fg="white"),
        ]
        table_rows.append(formatted_job)

    click.echo(tabulate(table_rows, headers=["Job id", "Connector id", "Index name", "Job status", "Job type", "Documents indexed", "Volume documents indexed (MiB)", "Documents deleted"]))

job.add_command(list)

@click.command(help="Cancel jobs. -i to pass connector name, -n to pass index name, -j to pass job id")
@click.pass_obj
@click.option('-i')
@click.option('-n')
@click.option('-j')
def cancel(obj, i, n, j):
    job_cli = Job(config=obj['config']['elasticsearch']) 
    click.confirm(click.style('Are you sure you want to cancel jobs?😱', fg='yellow'), abort=True)
    click.echo('Deleting jobs...')
    if job_cli.cancel(connector_id=i, index_name=n, job_id=j):
        click.echo(click.style("All jobs have been cancelled. You're incredible!", fg='green'))
    else:
        click.echo('')
        click.echo(click.style("Something went wrong. Please try again later or check your credentials", fg='red'), err=True)

job.add_command(cancel)

cli.add_command(job)

def main(args=None):
    cli()

if __name__ == '__main__':
    main()
