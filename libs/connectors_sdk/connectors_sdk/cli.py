#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from pathlib import Path
from typing_extensions import Annotated

from copier import run_copy, run_update
import typer

app = typer.Typer(
    no_args_is_help=True,
    rich_markup_mode="rich",
    epilog="Made with :heart: at [blue]Elastic[/blue]",
)

@app.command()
def docs():
   print("Opening documentation...")
   typer.launch("https://www.elastic.co/docs/reference/search-connectors")

@app.command()
def create(
    path: Annotated[Path, typer.Option(help="Path to create the data source in")] = None
):
    print("Creating data source")
    src_path = Path(__file__).parent / "template"
    dest_path = path or Path.cwd()
    run_copy(src_path=str(src_path), dst_path=dest_path)
    print(f"Data source created at: {dest_path}")

@app.command()
def update(
    source_name: Annotated[str, typer.Argument(help="Name of the data source")],
    path: Annotated[Path, typer.Option(help="Path where the data sources are")] = None
):
    print("Updating data source")
    dest_path = (path or Path.cwd()) / source_name
    run_update(dst_path=dest_path, src_path = str(Path(__file__).parent / "template"))

if __name__ == "__main__":
    app()
