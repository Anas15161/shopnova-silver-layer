"""Base class providing common transformation, deduplication, and cleansing operations."""

from abc import ABC, abstractmethod
from typing import Any, Dict
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


class BaseTransformer(ABC):
    """Abstract base class for dataset transformers in the Silver layer."""

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Executes full transformation pipeline on the input canonical DataFrame."""
        pass

    @staticmethod
    def deduplicate_by_lsn(
        df: DataFrame, primary_key: str, lsn_col: str = "_source_lsn"
    ) -> DataFrame:
        """Applies the CDC Golden Rule: keeps only the latest event per primary key based on LSN.

        If LSN is identical or missing, falls back on _ingested_at descending.
        """
        window_spec = Window.partitionBy(primary_key).orderBy(
            F.col(lsn_col).desc(), F.col("_ingested_at").desc()
        )
        return (
            df.withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

    @staticmethod
    def trim_all_strings(df: DataFrame) -> DataFrame:
        """Trims whitespace from all string columns in the DataFrame."""
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, F.trim(F.col(field.name)))
        return df

    @staticmethod
    def fill_null_defaults(df: DataFrame, defaults: Dict[str, Any]) -> DataFrame:
        """Replaces null values with specified domain defaults."""
        return df.fillna(defaults)
