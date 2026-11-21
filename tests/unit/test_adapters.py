"""Tests unitaires pour la fabrique et les interfaces d'adaptateurs sources."""

from unittest.mock import MagicMock
import pytest
from src.adapters.csv_adapter import CSVSourceAdapter
from src.adapters.factory import SourceAdapterFactory
from src.adapters.iceberg_adapter import IcebergSourceAdapter
from src.adapters.json_adapter import JSONSourceAdapter
from src.adapters.parquet_adapter import ParquetSourceAdapter


@pytest.fixture
def mock_spark():
    """Mock d'une session Spark pour les tests unitaires hôte."""
    return MagicMock()


def test_factory_returns_csv_adapter(mock_spark):
    adapter = SourceAdapterFactory.get_adapter("csv", mock_spark)
    assert isinstance(adapter, CSVSourceAdapter)
    assert adapter.format_name == "csv"


def test_factory_returns_json_adapter(mock_spark):
    adapter = SourceAdapterFactory.get_adapter("json", mock_spark)
    assert isinstance(adapter, JSONSourceAdapter)
    assert adapter.format_name == "json"


def test_factory_returns_parquet_adapter(mock_spark):
    adapter = SourceAdapterFactory.get_adapter("parquet", mock_spark)
    assert isinstance(adapter, ParquetSourceAdapter)
    assert adapter.format_name == "parquet"


def test_factory_returns_iceberg_adapter(mock_spark):
    adapter = SourceAdapterFactory.get_adapter("iceberg", mock_spark)
    assert isinstance(adapter, IcebergSourceAdapter)
    assert adapter.format_name == "iceberg"


def test_factory_case_insensitive(mock_spark):
    adapter = SourceAdapterFactory.get_adapter("  PARQUET  ", mock_spark)
    assert isinstance(adapter, ParquetSourceAdapter)


def test_factory_unsupported_format(mock_spark):
    with pytest.raises(ValueError, match="Format de source non supporte"):
        SourceAdapterFactory.get_adapter("xml", mock_spark)


def test_detect_format_from_path():
    assert SourceAdapterFactory.detect_format_from_path("s3a://bronze/orders.csv") == "csv"
    assert SourceAdapterFactory.detect_format_from_path("/data/stream.json") == "json"
    assert SourceAdapterFactory.detect_format_from_path("/data/stream.jsonl") == "json"
    assert SourceAdapterFactory.detect_format_from_path("s3a://bronze/orders.parquet") == "parquet"
    assert SourceAdapterFactory.detect_format_from_path("lake.silver.orders") == "iceberg"
