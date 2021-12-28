# type: ignore

import logging
from typing import Optional

import click
import click_log
import dask.dataframe
from click import Context

from pc_isd import Client, Config

logger = logging.getLogger("pc_isd")
click_log.basic_config(logger)


@click.group()
@click_log.simple_verbosity_option(logger)
@click.argument("CONFIG_PATH")
@click.pass_context
def main(context: Context, config_path: str) -> None:
    """Work with NOAA ISD data on the Planetary Computer from the command line."""
    config = Config.from_path(config_path)
    context.obj = config


@main.command()
@click.pass_context
def test_dask(context: Context) -> None:
    """Tests the Dask connection."""
    cluster = context.obj.start_dask_cluster()
    client = cluster.get_client()
    print(client.get_versions())


@main.command()
@click.option("-p", "--prefix", default="data")
@click.option("-l", "--limit", default=None, type=int)
@click.option("-y", "--year", default=None, type=int)
@click.pass_context
def list(
    context: Context, str, prefix: str, limit: Optional[int], year: Optional[int]
) -> None:
    """List ISD files."""
    reader = context.obj.reader(prefix=prefix, limit=limit)
    if year:
        prefix = f"{prefix}/{year}"
    for path in reader.list():
        print(path)


@main.command()
@click.option("--source-prefix", default="data")
@click.option("--target-prefix", default=None, type=str)
@click.option("-l", "--limit", default=None, type=int)
@click.option("-y", "--year", default=None, type=int)
@click.option("--overwrite/--no-overwrite", default=False)
@click.pass_context
def convert(
    context: Context,
    source_prefix: str,
    target_prefix: Optional[str],
    limit: Optional[int],
    year: Optional[int],
    overwrite: bool,
) -> None:
    """Converts ISD files to a parquet table."""
    if year:
        source_prefix = f"{source_prefix}/{year}"
    converter = context.obj.converter(
        source_prefix=source_prefix, source_limit=limit, target_prefix=target_prefix
    )
    cluster = context.obj.start_dask_cluster()
    print(f"Dask cluster started, dashboard here: {cluster.dashboard_link}")
    try:
        converter.convert(overwrite=overwrite)
    finally:
        cluster.shutdown()


@main.command()
@click.option("--prefix", default=None, type=str)
@click.pass_context
def show(context: Context, prefix: Optional[str]):
    """Shows the dataframe currently residing at the target."""
    writer: Client = context.obj.writer(prefix=prefix)
    data_frame = dask.dataframe.read_parquet(
        writer.adlfs_path(), storage_options=writer.adlfs_options()
    )
    print(data_frame.compute())
