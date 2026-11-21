"""Tests unitaires pour la gestion des schémas canoniques et types."""

import pytest
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)
from src.schemas.models import ColumnDefinition, TableSchemaConfig
from src.schemas.type_converter import UnsupportedTypeException, parse_spark_type
from src.schemas.manager import SchemaManager


def test_parse_spark_type_basics():
    assert isinstance(parse_spark_type("string"), StringType)
    assert isinstance(parse_spark_type("BIGINT"), LongType)
    assert isinstance(parse_spark_type("integer"), IntegerType)
    assert isinstance(parse_spark_type("boolean"), BooleanType)
    assert isinstance(parse_spark_type("timestamp"), TimestampType)


def test_parse_spark_type_decimal():
    dec_type = parse_spark_type("decimal(12, 2)")
    assert isinstance(dec_type, DecimalType)
    assert dec_type.precision == 12
    assert dec_type.scale == 2


def test_parse_spark_type_unsupported():
    with pytest.raises(UnsupportedTypeException, match="Type de donnee non supporte"):
        parse_spark_type("unsupported_custom_type")


def test_table_schema_config_validation():
    col1 = ColumnDefinition(name="id", type="bigint", required=True)
    col2 = ColumnDefinition(name="name", type="string", required=False, source_fields=["raw_name"])
    config = TableSchemaConfig(table_name="test_table", primary_key="id", columns=[col1, col2])
    assert config.table_name == "test_table"
    assert config.mandatory_column_names == ["id"]
    assert config.get_column("name").source_fields == ["raw_name"]


def test_load_orders_yaml(configs_dir):
    manager = SchemaManager(schemas_dir=configs_dir / "schemas")
    schema = manager.load_schema("orders")
    assert schema.table_name == "orders"
    assert schema.primary_key == "order_id"
    assert "order_id" in schema.mandatory_column_names
    assert "total_amount" in schema.mandatory_column_names
    assert schema.cdc_metadata.enabled is True
