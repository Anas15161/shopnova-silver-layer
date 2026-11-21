"""Base class for source data adapters."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from pyspark.sql import DataFrame, SparkSession


class BaseSourceAdapter(ABC):
    """Abstract base class defining the contract for source data readers."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def read(self, path: str, options: Optional[Dict[str, Any]] = None) -> DataFrame:
        """Reads data from the specified path and returns a PySpark DataFrame."""
        pass

    @property
    @abstractmethod
    def format_name(self) -> str:
        """Returns the format name handled by this adapter."""
        pass
