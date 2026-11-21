"""Factory for instantiating source adapters based on format or path."""

from typing import Dict, Type
from pyspark.sql import SparkSession
from src.adapters.base import BaseSourceAdapter
from src.adapters.csv_adapter import CSVSourceAdapter
from src.adapters.json_adapter import JSONSourceAdapter
from src.adapters.parquet_adapter import ParquetSourceAdapter
from src.adapters.iceberg_adapter import IcebergSourceAdapter


class SourceAdapterFactory:
    """Registry and factory for source adapters."""

    _ADAPTERS: Dict[str, Type[BaseSourceAdapter]] = {
        "csv": CSVSourceAdapter,
        "json": JSONSourceAdapter,
        "parquet": ParquetSourceAdapter,
        "iceberg": IcebergSourceAdapter,
    }

    @classmethod
    def get_adapter(cls, format_type: str, spark: SparkSession) -> BaseSourceAdapter:
        """Returns the appropriate adapter instance for the requested format."""
        normalized = format_type.strip().lower()
        adapter_cls = cls._ADAPTERS.get(normalized)
        if not adapter_cls:
            supported = ", ".join(cls._ADAPTERS.keys())
            raise ValueError(
                f"Format de source non supporte: '{format_type}'. Formats disponibles: {supported}"
            )
        return adapter_cls(spark)

    @classmethod
    def detect_format_from_path(cls, path: str) -> str:
        """Infers the format from a file path or table identifier."""
        cleaned = path.split("?")[0].strip().lower()
        if cleaned.endswith(".csv"):
            return "csv"
        if cleaned.endswith(".json") or cleaned.endswith(".jsonl"):
            return "json"
        if cleaned.endswith(".parquet") or cleaned.endswith(".pq"):
            return "parquet"
        if "." in path and not ("/" in path or "\\" in path):
            return "iceberg"
        return "parquet"
