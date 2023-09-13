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
from connectors.cli.auth import Auth
from connectors import __version__  # NOQA
from connectors.cli.connector import Connector
from tabulate import tabulate


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
    if config:
        ctx.ensure_object(dict)
        ctx.obj['config'] = yaml.safe_load(config)
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

@click.command()
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


connector.add_command(list)

cli.add_command(connector)

# Index group
@click.group(invoke_without_command=True)
@click.pass_obj
def index(obj):
    click.echo('testing index')

cli.add_command(index)

# Job group
@click.group(invoke_without_command=True)
@click.pass_obj
def job(obj):
    click.echo('testing job')

cli.add_command(job)

def main(args=None):
    cli()

if __name__ == '__main__':
    main()
