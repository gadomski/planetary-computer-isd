import logging
import os.path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple

import dask
import dask.dataframe
import dask.delayed
import pandas
from dask_gateway import GatewayCluster
from pandas import DataFrame

from .client import Client

logger = logging.getLogger(__name__)


class Converter:
    """Converts source ISD files to parquet tables."""

    def __init__(
        self,
        cluster: GatewayCluster,
        reader: Client,
        writer: Client,
        periods: int,
    ):
        self._cluster = cluster
        self._reader = reader
        self._writer = writer
        self._periods = periods

    def convert(self, overwrite: bool = False) -> None:
        existing_paths = list(self._writer.list())
        if existing_paths:
            if overwrite:
                logger.warn(
                    f"Overwriting {len(existing_paths)} paths in "
                    f"destination at prefix '{self._writer.prefix()}'"
                )
                self._writer.rm(existing_paths)
            else:
                raise Exception(
                    f"{len(existing_paths)} exist in prefix "
                    f"'{self._writer.prefix()}', but overwrite=False"
                )
        paths = list(self._reader.list())
        logger.info(f"Found {len(paths)} paths from reader")
        years = separate_into_years(paths)
        logger.info(
            f"Found {len(years)} years (from {min(years.keys())} to {max(years.keys())})"
        )
        append = False
        client = self._cluster.get_client()
        for year in sorted(years):
            logger.info(f"Beginning {year} ({len(paths)} paths)")
            full_year_delayed = [
                dask.delayed(self._reader.read_data_frame)(path) for path in years[year]
            ]
            full_year_persisted = dask.persist(*full_year_delayed)
            for start, end in intervals(year, self._periods):
                windowed_delayed = [
                    dask.delayed(window)(data_frame, start, end)
                    for data_frame in full_year_delayed
                ]
                data_frame = dask.dataframe.from_delayed(windowed_delayed).set_index(
                    "timestamp", divisions=[start, end]
                )
                logger.info(
                    f"Writing parquet between {start} and {end} to {self._writer.adlfs_path()} (append={append})"
                )
                future = client.submit(
                    dask.dataframe.to_parquet,
                    data_frame,
                    self._writer.adlfs_path(),
                    append=append,
                    engine="pyarrow",
                    storage_options=self._writer.adlfs_options(),
                )
                future.result()
                logger.debug("Done writing")
                if not append:
                    append = True


def separate_into_years(paths: List[str]) -> Dict[int, List[str]]:
    years = defaultdict(list)
    for path in paths:
        year = int(os.path.basename(os.path.dirname(path)))
        years[year].append(path)
    return years


def intervals(year: int, periods: int) -> List[Tuple[datetime, datetime]]:
    """Returns reasonable parquet partition intervals for the given year."""
    range = list(
        pandas.date_range(
            start=datetime(year, 1, 1),
            end=datetime(year + 1, 1, 1),
            periods=periods,
        ).normalize()
    )
    return [(a, b) for a, b in zip(range, range[1:])]


def window(data_frame: DataFrame, start: datetime, end: datetime) -> DataFrame:
    """Returns a copy of this data frame, reduced to fall within the given window."""
    return data_frame[data_frame["timestamp"].between(start, end, inclusive="left")]
