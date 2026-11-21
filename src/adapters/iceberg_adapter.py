"""Source adapter for Apache Iceberg tables."""

from typing import Any, Dict, Optional
from pyspark.sql import DataFrame
from src.adapters.base import BaseSourceAdapter


class IcebergSourceAdapter(BaseSourceAdapter):
    """Adapter for reading Apache Iceberg tables (snapshots, time travel)."""

    @property
    def format_name(self) -> str:
        return "iceberg"

    def read(self, path: str, options: Optional[Dict[str, Any]] = None) -> DataFrame:
        reader = self.spark.read.format("iceberg")
        if options:
            if "as_of_timestamp" in options:
                reader = reader.option("as-of-timestamp", str(options["as_of_timestamp"]))
            if "snapshot_id" in options:
                reader = reader.option("snapshot-id", str(options["snapshot_id"]))

        # S'il s'agit d'un identifiant de table de catalogue (ex: lake.silver.orders)
        if "." in path and not path.startswith("s3") and not path.startswith("file"):
            return reader.table(path)
        return reader.load(path)
