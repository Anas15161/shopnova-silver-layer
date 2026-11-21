"""Source adapter for CSV format."""

from typing import Any, Dict, Optional
from pyspark.sql import DataFrame
from src.adapters.base import BaseSourceAdapter


class CSVSourceAdapter(BaseSourceAdapter):
    """Adapter for reading CSV source files."""

    @property
    def format_name(self) -> str:
        return "csv"

    def read(self, path: str, options: Optional[Dict[str, Any]] = None) -> DataFrame:
        opts = {
            "header": "true",
            "inferSchema": "true",
            "mode": "PERMISSIVE",
        }
        if options:
            opts.update(options)

        reader = self.spark.read.format("csv")
        for key, val in opts.items():
            reader = reader.option(key, str(val))

        return reader.load(path)
