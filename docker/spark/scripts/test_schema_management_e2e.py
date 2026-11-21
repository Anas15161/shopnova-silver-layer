"""Test d'integration end-to-end pour la validation et le mapping de schemas (Phase 3)."""

import sys
if "/home/iceberg" not in sys.path:
    sys.path.append("/home/iceberg")

from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, LongType, TimestampType
from src.adapters.factory import SourceAdapterFactory
from src.schemas.manager import SchemaManager, SchemaValidationError


def main():
    print("=" * 60)
    print("DEMARRAGE DU TEST E2E DU SCHEMA MANAGEMENT (PHASE 3)")
    print("=" * 60)

    spark = SparkSession.builder.appName("ShopNova-Schema-Test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema_mgr = SchemaManager(schemas_dir="/home/iceberg/configs/schemas")
    orders_schema = schema_mgr.load_schema("orders")
    print(f"[1/4] Schema 'orders' charge avec succes (Version {orders_schema.version}).")
    print(f"      Colonnes obligatoires: {orders_schema.mandatory_column_names}")

    # 1. Validation & Transformation sur flux CDC Debezium JSON
    print("\n[2/4] Test de mapping sur evenements CDC Debezium (JSON)...")
    json_adapter = SourceAdapterFactory.get_adapter("json", spark)
    df_raw_cdc = json_adapter.read("/home/iceberg/data/bronze_sample/orders_cdc.json")
    
    df_silver_cdc = schema_mgr.validate_and_transform(df_raw_cdc, orders_schema)
    print("  -> Schema Silver canonique produit depuis CDC :")
    df_silver_cdc.printSchema()
    df_silver_cdc.show(truncate=False)

    # Assertions sur les types et colonnes
    fields_dict = {f.name: f.dataType for f in df_silver_cdc.schema.fields}
    assert isinstance(fields_dict["order_id"], LongType), "order_id doit etre LongType"
    assert isinstance(fields_dict["total_amount"], DecimalType), "total_amount doit etre DecimalType"
    assert isinstance(fields_dict["created_at"], TimestampType), "created_at doit etre TimestampType"
    assert "_source_lsn" in fields_dict, "_source_lsn manquant"
    assert "_last_op" in fields_dict, "_last_op manquant"
    assert "is_deleted" in fields_dict, "is_deleted manquant"
    assert "_ingested_at" in fields_dict, "_ingested_at manquant"

    # Vérification des valeurs extraites de l'enveloppe Debezium
    orders_rows = df_silver_cdc.collect()
    assert orders_rows[0]["order_id"] == 2001
    assert orders_rows[0]["_source_lsn"] == 1001
    assert orders_rows[0]["_last_op"] == "c"
    assert orders_rows[1]["_last_op"] == "u"

    # 2. Validation & Transformation sur format plat CSV (Fallback mapping)
    print("\n[3/4] Test de mapping sur source plate CSV (fallback transparent)...")
    csv_adapter = SourceAdapterFactory.get_adapter("csv", spark)
    df_raw_csv = csv_adapter.read("/home/iceberg/data/bronze_sample/orders_sample.csv")
    df_silver_csv = schema_mgr.validate_and_transform(df_raw_csv, orders_schema)
    print("  -> Schema Silver canonique produit depuis CSV :")
    df_silver_csv.show(3)
    assert df_silver_csv.count() == 3
    assert set(df_silver_csv.columns) == set(df_silver_cdc.columns)

    # 3. Test de détection de violation de contrat (SchemaValidationError)
    print("\n[4/4] Test de detection de violation de schema...")
    # Création d'un DataFrame corrompu sans 'order_id'
    df_corrupted = df_raw_csv.drop("order_id")
    try:
        schema_mgr.validate_and_transform(df_corrupted, orders_schema)
        print("❌ ERREUR: La violation de schema n'a pas ete detectee !")
        sys.exit(1)
    except SchemaValidationError as e:
        print(f"  -> Rejet confirme : {e}")

    print("\n" + "=" * 60)
    print("✅ TOUS LES TESTS DU SCHEMA MANAGEMENT ONT REUSSI !")
    print("Contrats YAML, mapping transparent CDC/Flat, casting strict et rejets validés.")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
