"""Test d'integration end-to-end pour les Source Adapters PySpark."""

import sys

# Assure la disponibilite du package src dans le PYTHONPATH
if "/home/iceberg" not in sys.path:
    sys.path.append("/home/iceberg")

from pyspark.sql import SparkSession
from src.adapters.factory import SourceAdapterFactory


def main():
    print("=" * 60)
    print("DEMARRAGE DU TEST E2E DES SOURCE ADAPTERS (PHASE 2)")
    print("=" * 60)

    spark = SparkSession.builder.appName("ShopNova-Adapters-Test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Test CSVSourceAdapter
    print("[1/5] Test du CSVSourceAdapter...")
    csv_adapter = SourceAdapterFactory.get_adapter("csv", spark)
    df_csv = csv_adapter.read("/home/iceberg/data/bronze_sample/orders_sample.csv")
    csv_count = df_csv.count()
    print(f"  -> Lignes lues depuis CSV: {csv_count}")
    df_csv.show(3)
    assert csv_count == 3, f"Attendu 3 lignes CSV, recu {csv_count}"
    assert "order_id" in df_csv.columns

    # 2. Test JSONSourceAdapter (CDC Debezium)
    print("[2/5] Test du JSONSourceAdapter (evenements CDC)...")
    json_adapter = SourceAdapterFactory.get_adapter("json", spark)
    df_json = json_adapter.read("/home/iceberg/data/bronze_sample/orders_cdc.json")
    json_count = df_json.count()
    print(f"  -> Lignes lues depuis JSON CDC: {json_count}")
    df_json.show(3, truncate=False)
    assert json_count == 3, f"Attendu 3 lignes JSON, recu {json_count}"
    assert "op" in df_json.columns
    assert "source" in df_json.columns
    assert "after" in df_json.columns

    # 3. Test ParquetSourceAdapter
    print("[3/5] Test du ParquetSourceAdapter...")
    parquet_path = "/tmp/test_orders.parquet"
    df_csv.write.mode("overwrite").parquet(parquet_path)
    parquet_adapter = SourceAdapterFactory.get_adapter("parquet", spark)
    df_parquet = parquet_adapter.read(parquet_path)
    parquet_count = df_parquet.count()
    print(f"  -> Lignes lues depuis Parquet: {parquet_count}")
    assert parquet_count == 3, f"Attendu 3 lignes Parquet, recu {parquet_count}"

    # 4. Test IcebergSourceAdapter
    print("[4/5] Test du IcebergSourceAdapter...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.test_adapters")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lake.test_adapters.sample_iceberg (
            id INT,
            name STRING,
            created_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')
    """
    )
    spark.sql("INSERT INTO lake.test_adapters.sample_iceberg VALUES (101, 'Item A', current_timestamp())")
    spark.sql("INSERT INTO lake.test_adapters.sample_iceberg VALUES (102, 'Item B', current_timestamp())")

    iceberg_adapter = SourceAdapterFactory.get_adapter("iceberg", spark)
    df_iceberg = iceberg_adapter.read("lake.test_adapters.sample_iceberg")
    iceberg_count = df_iceberg.count()
    print(f"  -> Lignes lues depuis table Iceberg: {iceberg_count}")
    df_iceberg.show()
    assert iceberg_count == 2, f"Attendu 2 lignes Iceberg, recu {iceberg_count}"

    # Nettoyage Iceberg
    spark.sql("DROP TABLE lake.test_adapters.sample_iceberg")
    spark.sql("DROP NAMESPACE lake.test_adapters")

    # 5. Test Factory Auto-detection
    print("[5/5] Test de l'auto-detection de format via Factory...")
    auto_csv = SourceAdapterFactory.detect_format_from_path("s3a://bronze/data.csv")
    auto_json = SourceAdapterFactory.detect_format_from_path("s3a://bronze/events.json")
    auto_parquet = SourceAdapterFactory.detect_format_from_path("s3a://bronze/data.parquet")
    auto_iceberg = SourceAdapterFactory.detect_format_from_path("lake.silver.orders")
    assert auto_csv == "csv"
    assert auto_json == "json"
    assert auto_parquet == "parquet"
    assert auto_iceberg == "iceberg"

    print("=" * 60)
    print("✅ TOUS LES TESTS DES SOURCE ADAPTERS ONT REUSSI !")
    print("CSV, JSON (CDC), Parquet et Iceberg sont geres de maniere agnostique.")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
