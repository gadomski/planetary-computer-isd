from typing import Optional

from dask.distributed import PipInstall
from dask_gateway import Gateway, GatewayCluster
from serde import serde
from serde.toml import from_toml

from .client import Client
from .converter import Converter


@serde
class BlobStorage:
    """Blob storage configuration"""

    account_name: str
    container_name: str
    sas: Optional[str]


@serde
class Dask:
    """Dask configuration"""

    num_workers: int


@serde
class Config:
    """Configuration dataclass."""

    source: BlobStorage
    target: BlobStorage
    dask: Dask

    @classmethod
    def from_path(cls, path: str) -> "Config":
        """Reads configuration from a TOML path."""
        with open(path) as file:
            config = from_toml(cls, file.read())
            assert isinstance(config, cls)
            return config

    def reader(
        self, prefix: Optional[str] = None, limit: Optional[int] = None
    ) -> Client:
        """Returns a client for reading data in this configuration."""
        return Client(
            self.source.account_name,
            self.source.container_name,
            self.source.sas,
            prefix=prefix,
            limit=limit,
        )

    def writer(self, prefix: Optional[str] = None) -> Client:
        """Returns a client for writing data in this configuration."""
        return Client(
            self.target.account_name,
            self.target.container_name,
            self.target.sas,
            prefix=prefix,
        )

    def converter(
        self,
        source_prefix: Optional[str] = None,
        source_limit: Optional[int] = None,
        target_prefix: Optional[str] = None,
    ) -> Converter:
        """Returns a converter for this configuration."""
        return Converter(
            self.reader(prefix=source_prefix, limit=source_limit),
            self.writer(prefix=target_prefix),
        )

    def start_dask_cluster(self) -> GatewayCluster:
        """Starts a Dask cluster."""
        gateway = Gateway()
        cluster = gateway.new_cluster()
        plugin = PipInstall(
            packages=[
                "git+https://github.com/gadomski/planetary-computer-isd",
            ],
            pip_options=["--upgrade"],
        )
        client = cluster.get_client()
        client.register_worker_plugin(plugin)
        cluster.scale(self.dask.num_workers)
        return cluster
