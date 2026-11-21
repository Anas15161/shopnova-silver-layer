"""Source adapter for Parquet format."""

from typing import Any, Dict, Optional
from pyspark.sql import DataFrame
from src.adapters.base import BaseSourceAdapter


class ParquetSourceAdapter(BaseSourceAdapter):
    """Adapter for reading Parquet files."""

    @property
    def format_name(self) -> str:
        return "parquet"

    def read(self, path: str, options: Optional[Dict[str, Any]] = None) -> DataFrame:
        opts = {
            "mergeSchema": "false",
        }
        if options:
            opts.update(options)

        reader = self.spark.read.format("parquet")
        for key, val in opts.items():
            reader = reader.option(key, str(val))

        return reader.load(path)
