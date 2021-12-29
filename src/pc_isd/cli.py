# type: ignore

import logging
import sys
from typing import Optional

import click
import click_log
import dask.dataframe
from click import Context
from dask_gateway import Gateway

from pc_isd import Client, Config, Converter

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
@click.pass_context
def start_dask_cluster(context: Context) -> None:
    cluster = context.obj.start_dask_cluster()
    print(f"Cluster dashboard link: {cluster.dashboard_link}")


@main.command()
def stop_dask_clusters() -> None:
    gateway = Gateway()
    clusters = gateway.list_clusters()
    print(f"{len(clusters)} clusters found.")
    for cluster in clusters:
        print(f"Stopping {cluster.name}")
        cluster = gateway.connect(cluster.name)
        cluster.shutdown()
    print("Done!")


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
    gateway = Gateway()
    clusters = gateway.list_clusters()
    if not clusters:
        print("No clusters found in gateway, exiting!")
        sys.exit(1)
    elif len(clusters) != 1:
        print(f"More than one cluster found in gateway, exiting! {clusters}")
    else:
        cluster = gateway.connect(clusters[0].name)
    converter: Converter = context.obj.converter(
        cluster=cluster,
        source_prefix=source_prefix,
        source_limit=limit,
        target_prefix=target_prefix,
    )
    converter.convert(overwrite=overwrite)


@main.command()
@click.option("--prefix", default=None, type=str)
@click.pass_context
def show(context: Context, prefix: Optional[str]):
    """Shows the dataframe currently residing at the target."""
    writer: Client = context.obj.writer(prefix=prefix)
    data_frame = dask.dataframe.read_parquet(
        writer.adlfs_path(), storage_options=writer.adlfs_options(), engine="pyarrow"
    )
    print(data_frame.compute())
