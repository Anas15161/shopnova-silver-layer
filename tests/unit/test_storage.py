"""Tests unitaires pour le module de stockage Apache Iceberg."""

from datetime import datetime, timezone
from unittest.mock import MagicMock
from src.storage.iceberg_writer import IcebergWriter
from src.storage.table_manager import IcebergTableManager
from src.storage.time_travel import IcebergTimeTravel


def test_iceberg_table_manager_namespace():
    mock_spark = MagicMock()
    mgr = IcebergTableManager(spark=mock_spark, catalog="lake", namespace="silver")
    assert mgr.full_namespace == "lake.silver"

    mgr.ensure_namespace()
    mock_spark.sql.assert_called_with("CREATE NAMESPACE IF NOT EXISTS lake.silver")


def test_iceberg_writer_merge_sql_generation():
    mock_spark = MagicMock()
    writer = IcebergWriter(spark=mock_spark, catalog="lake", namespace="silver")

    sql = writer.build_merge_sql(
        staging_view="_staging_orders_123",
        table_name="orders",
        columns=["order_id", "status", "total_amount", "_source_lsn", "is_deleted"],
        primary_keys=["order_id"],
    )

    assert "MERGE INTO lake.silver.orders AS target" in sql
    assert "USING _staging_orders_123 AS source" in sql
    assert "ON target.order_id = source.order_id" in sql
    assert "WHEN MATCHED AND source.is_deleted = true THEN" in sql
    assert "DELETE" in sql
    assert "source._source_lsn >= target._source_lsn" in sql
    assert "target.status = source.status" in sql
    assert "INSERT (order_id, status, total_amount, _source_lsn, is_deleted)" in sql


def test_iceberg_writer_composite_key():
    mock_spark = MagicMock()
    writer = IcebergWriter(spark=mock_spark, catalog="lake", namespace="silver")

    sql = writer.build_merge_sql(
        staging_view="_staging_order_items",
        table_name="order_items",
        columns=["order_id", "item_id", "quantity", "_source_lsn", "is_deleted"],
        primary_keys=["order_id", "item_id"],
    )

    assert "ON target.order_id = source.order_id AND target.item_id = source.item_id" in sql


def test_time_travel_timestamp_parsing():
    mock_spark = MagicMock()
    tt = IcebergTimeTravel(spark=mock_spark, catalog="lake", namespace="silver")

    # Test int (ms)
    tt.read_at_timestamp("orders", 1700000000000)
    mock_spark.read.option.assert_called_with("as-of-timestamp", 1700000000000)

    # Test datetime
    dt = datetime(2026, 11, 21, 12, 0, 0, tzinfo=timezone.utc)
    expected_ms = int(dt.timestamp() * 1000)
    tt.read_at_timestamp("orders", dt)
    mock_spark.read.option.assert_called_with("as-of-timestamp", expected_ms)
