"""Quarantine storage manager for isolated invalid records."""

import os
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class QuarantineManager:
    """Handles routing and persisting non-compliant records to quarantine storage."""

    def __init__(self, quarantine_target: Optional[str] = None):
        if quarantine_target is None:
            self.quarantine_target = os.getenv("QUARANTINE_TARGET", "lake.quarantine")
        else:
            self.quarantine_target = quarantine_target

    def save_to_quarantine(self, df: DataFrame, table_name: str) -> str:
        """Writes quarantined records either to an Iceberg table or a filesystem path.

        Args:
            df: DataFrame of rejected records with _rejection_reason and metadata.
            table_name: Name of the dataset (e.g. orders).

        Returns:
            The destination identifier (Iceberg table name or URI).
        """
        if "quarantined_date" not in df.columns:
            write_df = df.withColumn("quarantined_date", F.to_date(F.current_timestamp()))
        else:
            write_df = df

        if self.quarantine_target.startswith("/") or self.quarantine_target.startswith("file://"):
            dest_uri = f"{self.quarantine_target.rstrip('/')}/{table_name}"
            (write_df.write.mode("append").partitionBy("quarantined_date").parquet(dest_uri))
            return dest_uri
        else:
            # Iceberg catalog namespace (e.g. lake.quarantine)
            catalog_namespace = self.quarantine_target.rstrip(".")
            full_table_name = f"{catalog_namespace}.{table_name}"

            spark = df.sparkSession
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_namespace}")

            try:
                write_df.writeTo(full_table_name).append()
            except Exception:
                (
                    write_df.writeTo(full_table_name)
                    .tableProperty("format-version", "2")
                    .partitionedBy("quarantined_date")
                    .createOrReplace()
                )
            return full_table_name
