"""ACID Merge and write operations for Apache Iceberg Silver tables."""

import os
import uuid
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession


class IcebergWriter:
    """Handles transactional ACID writes, CDC merges, and partition overwrites."""

    DEFAULT_PRIMARY_KEYS: Dict[str, List[str]] = {
        "orders": ["order_id"],
        "customers": ["customer_id"],
        "products": ["product_id"],
    }

    def __init__(
        self,
        spark: SparkSession,
        catalog: Optional[str] = None,
        namespace: Optional[str] = None,
    ):
        self.spark = spark
        self.catalog = catalog or os.getenv("ICEBERG_CATALOG", "lake")
        self.namespace = namespace or os.getenv("SILVER_NAMESPACE", "silver")

    @property
    def full_namespace(self) -> str:
        return f"{self.catalog}.{self.namespace}"

    def build_merge_sql(
        self,
        staging_view: str,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
    ) -> str:
        """Constructs an idempotent ACID MERGE INTO SQL statement with CDC LSN ordering."""
        target_table = f"{self.full_namespace}.{table_name}"

        # Join condition on primary keys
        on_clause = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])

        # Exclude primary keys from the UPDATE assignment list
        update_cols = [col for col in columns if col not in primary_keys]
        update_clause = ", ".join([f"target.{col} = source.{col}" for col in update_cols])

        # Insert columns
        cols_clause = ", ".join(columns)
        source_cols_clause = ", ".join([f"source.{col}" for col in columns])

        sql = f"""
        MERGE INTO {target_table} AS target
        USING {staging_view} AS source
        ON {on_clause}
        WHEN MATCHED AND source.is_deleted = true THEN
            DELETE
        WHEN MATCHED AND (
            source._source_lsn >= target._source_lsn OR target._source_lsn IS NULL
        ) THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED AND (source.is_deleted = false OR source.is_deleted IS NULL) THEN
            INSERT ({cols_clause}) VALUES ({source_cols_clause})
        """
        return sql.strip()

    def merge(
        self,
        df: DataFrame,
        table_name: str,
        primary_keys: Optional[List[str]] = None,
    ) -> int:
        """Executes an ACID MERGE INTO operation respecting CDC LSN sequencing.

        Args:
            df: Cleaned and validated PySpark DataFrame.
            table_name: Silver table name (e.g. orders).
            primary_keys: List of primary key columns. Resolves from defaults if omitted.

        Returns:
            The number of input records processed in the merge batch.
        """
        if primary_keys is None:
            primary_keys = self.DEFAULT_PRIMARY_KEYS.get(table_name, ["id"])

        count = df.count()
        if count == 0:
            return 0

        # Unique staging view name
        staging_view = f"_staging_{table_name}_{uuid.uuid4().hex[:8]}"
        df.createOrReplaceTempView(staging_view)

        try:
            # Query target table schema to ensure column alignment
            target_cols = self.spark.table(f"{self.full_namespace}.{table_name}").columns
            common_cols = [col for col in target_cols if col in df.columns]

            merge_query = self.build_merge_sql(
                staging_view=staging_view,
                table_name=table_name,
                columns=common_cols,
                primary_keys=primary_keys,
            )
            self.spark.sql(merge_query)
        finally:
            self.spark.catalog.dropTempView(staging_view)

        return count

    def append(self, df: DataFrame, table_name: str) -> None:
        """Appends records to the Iceberg table."""
        full_table = f"{self.full_namespace}.{table_name}"
        df.writeTo(full_table).append()

    def overwrite_partitions(self, df: DataFrame, table_name: str) -> None:
        """Dynamically overwrites matching partitions."""
        full_table = f"{self.full_namespace}.{table_name}"
        df.writeTo(full_table).overwritePartitions()
