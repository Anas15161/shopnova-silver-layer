"""Iceberg table DDL, partition strategies, and maintenance manager."""

import os
from typing import Dict, List, Optional
from pyspark.sql import SparkSession


class IcebergTableManager:
    """Manages Silver layer Apache Iceberg table schemas, partitioning, and maintenance."""

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

    def ensure_namespace(self) -> None:
        """Creates the Silver namespace if it does not already exist."""
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self.full_namespace}")

    def create_orders_table(self) -> None:
        """Creates the silver.orders Iceberg table.

        Partitioned by created_date for optimal temporal range scans.
        Configured with Merge-On-Read (MOR) for high-throughput CDC updates and deletes.
        """
        self.ensure_namespace()
        table_name = f"{self.full_namespace}.orders"
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            order_id BIGINT NOT NULL,
            customer_id BIGINT NOT NULL,
            status STRING,
            total_amount DECIMAL(12, 2),
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            created_date DATE,
            is_paid BOOLEAN,
            is_final_status BOOLEAN,
            is_deleted BOOLEAN,
            _last_op STRING,
            _source_lsn BIGINT,
            _ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (created_date)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.merge.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.parquet.compression-codec' = 'zstd'
        )
        """
        self.spark.sql(ddl)

    def create_customers_table(self) -> None:
        """Creates the silver.customers Iceberg table.

        Dimension table with Merge-On-Read format-version 2.
        """
        self.ensure_namespace()
        table_name = f"{self.full_namespace}.customers"
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            customer_id BIGINT NOT NULL,
            email STRING,
            first_name STRING,
            last_name STRING,
            full_name STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            is_deleted BOOLEAN,
            _last_op STRING,
            _source_lsn BIGINT,
            _ingested_at TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.merge.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.parquet.compression-codec' = 'zstd'
        )
        """
        self.spark.sql(ddl)

    def create_products_table(self) -> None:
        """Creates the silver.products Iceberg table.

        Partitioned by category for catalog filtering.
        """
        self.ensure_namespace()
        table_name = f"{self.full_namespace}.products"
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            product_id BIGINT NOT NULL,
            name STRING,
            category STRING,
            price DECIMAL(10, 2),
            stock_quantity INT,
            is_in_stock BOOLEAN,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            is_deleted BOOLEAN,
            _last_op STRING,
            _source_lsn BIGINT,
            _ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (category)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.merge.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.parquet.compression-codec' = 'zstd'
        )
        """
        self.spark.sql(ddl)

    def create_all_silver_tables(self) -> None:
        """Creates all Silver layer Iceberg tables."""
        self.create_orders_table()
        self.create_customers_table()
        self.create_products_table()

    def rewrite_data_files(self, table_name: str) -> List[Dict]:
        """Triggers Iceberg small file compaction on a target table."""
        query = (
            f"CALL {self.catalog}.system.rewrite_data_files("
            f"table => '{self.namespace}.{table_name}')"
        )
        result_rows = self.spark.sql(query).collect()
        return [row.asDict() for row in result_rows]

    def expire_snapshots(self, table_name: str, older_than_timestamp: str) -> List[Dict]:
        """Expires old Iceberg table snapshots older than a specified ISO timestamp."""
        query = (
            f"CALL {self.catalog}.system.expire_snapshots("
            f"table => '{self.namespace}.{table_name}', "
            f"older_than => TIMESTAMP '{older_than_timestamp}')"
        )
        result_rows = self.spark.sql(query).collect()
        return [row.asDict() for row in result_rows]

    def drop_table(self, table_name: str, purge: bool = False) -> None:
        """Drops a Silver table, optionally purging data."""
        purge_clause = "PURGE" if purge else ""
        self.spark.sql(f"DROP TABLE IF EXISTS {self.full_namespace}.{table_name} {purge_clause}")
