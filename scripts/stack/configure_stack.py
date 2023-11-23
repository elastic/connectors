import click
import yaml

__all__ = ["main"]

DEFAULT_ES_HOST = "http://localhost:9200"
CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"
DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "..", "config.yml")

"""
1. Choose which third-party service you’d like to use by selecting a data source.
2. Create and name a new Elasticsearch index.
3. Generate a new API key and save it somewhere safe.
4. Name your connector and provide an optional description
5. Convert native connector to a connector client (Only applicable if connector is also available natively). This action is irreversible.
"""

# first arg should be path to temp dir / file to create


@click.command()
def set_elasticsearch_auth():
    click.echo("running set_elasticsearch_auth");


# Create user for connectors?
@click.command()
def create_index_api_key():
    click.echo("running create_index_api_key");

# Create index(es) for connectors?
@click.command()
def create_connector_index():
    click.echo("running create_connector_index");

# Create connectors?
@click.command()
def create_connector():
    click.echo("running create_connector");

# @TODO print help page when no arguments passed
@click.group(invoke_without_command=True)
@click.option('-v', '--version', is_flag=True, callback=print_version,
              expose_value=False, is_eager=True)
@click.argument('output_file')
@click.pass_context
def cli(ctx, output_file):
    if output:
        ctx.ensure_object(dict)
        ctx.obj['output'] = output
    pass

cli.add_command(index)

def main(args=None):
    cli()
    # write changes to a temp file that can be used for when we actually run the stack

if __name__ == '__main__':
    main()
