"""Source adapter for JSON format (CDC events / lines)."""

from typing import Any, Dict, Optional
from pyspark.sql import DataFrame
from src.adapters.base import BaseSourceAdapter


class JSONSourceAdapter(BaseSourceAdapter):
    """Adapter for reading JSON files and Debezium CDC event streams."""

    @property
    def format_name(self) -> str:
        return "json"

    def read(self, path: str, options: Optional[Dict[str, Any]] = None) -> DataFrame:
        opts = {
            "multiLine": "false",
            "mode": "PERMISSIVE",
        }
        if options:
            opts.update(options)

        reader = self.spark.read.format("json")
        for key, val in opts.items():
            reader = reader.option(key, str(val))

        return reader.load(path)
