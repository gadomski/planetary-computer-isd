import os.path
from typing import Any, Dict, Iterable, Optional

import isd.pandas
from azure.storage.blob import ContainerClient
from isd import Record
from pandas import DataFrame


class Client:
    """Wraps a container client with some helper methods."""

    def __init__(
        self,
        account_name: str,
        container_name: str,
        sas: Optional[str],
        prefix: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        self._account_name = account_name
        self._container_name = container_name
        self._sas = sas
        self._prefix = prefix
        self._limit = limit
        account_url = f"https://{account_name}.blob.core.windows.net"
        self._client = ContainerClient(account_url, container_name, credential=sas)

    def list(
        self,
    ) -> Iterable[str]:
        """Lists the paths inside of this reader w/ the configured prefix."""
        count = 0
        for blob in self._client.list_blobs(name_starts_with=self._prefix):
            count += 1
            yield blob.name
            if self._limit and count >= self._limit:
                break

    def prefix(self) -> Optional[str]:
        return self._prefix

    def rm(self, paths: Iterable[str]) -> None:
        """Removes paths."""
        self._client.delete_blobs(*paths)

    def read_data_frame(self, path: str) -> DataFrame:
        """Reads an ISD file into a DataFrame."""
        blob = self._client.download_blob(path)
        data = blob.readall()
        if os.path.splitext(path)[1] == ".gz":
            import gzip

            data = gzip.decompress(data)
        assert isinstance(data, bytes)
        data_frame = isd.pandas.data_frame(
            Record.parse(line) for line in data.decode("utf-8").splitlines()
        )
        return data_frame

    def adlfs_path(self) -> str:
        """Returns the adlfs path for this location."""
        path = f"az://{self._container_name}"
        if self._prefix:
            path = f"{path}/{self._prefix}"
        return path

    def adlfs_options(self) -> Dict[str, Any]:
        """Returns the adlfs storage options."""
        options = {
            "account_name": self._account_name,
            "use_listings_cache": False,
        }
        if self._sas:
            options["sas_token"] = self._sas
        return options
