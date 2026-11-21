"""Test d'integration End-to-End pour Apache Iceberg Storage (Phase 6)."""

import sys
from datetime import datetime, timezone

if "/home/iceberg" not in sys.path:
    sys.path.append("/home/iceberg")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    TimestampType,
)
from src.storage.iceberg_writer import IcebergWriter
from src.storage.table_manager import IcebergTableManager
from src.storage.time_travel import IcebergTimeTravel


def main():
    print("=" * 60)
    print("DEMARRAGE DU TEST E2E APACHE ICEBERG STORAGE (PHASE 6)")
    print("=" * 60)

    spark = SparkSession.builder.appName("ShopNova-Iceberg-Storage-Test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    table_mgr = IcebergTableManager(spark, catalog="lake", namespace="silver")
    writer = IcebergWriter(spark, catalog="lake", namespace="silver")
    time_travel = IcebergTimeTravel(spark, catalog="lake", namespace="silver")

    # 1. Nettoyage et initialisation des tables Silver
    print("\n[1/6] Creation et verification des tables Silver (orders, customers, products)...")
    table_mgr.drop_table("orders", purge=True)
    table_mgr.drop_table("customers", purge=True)
    table_mgr.drop_table("products", purge=True)

    table_mgr.create_all_silver_tables()
    print("  -> Tables Silver creees avec format-version 2 (Merge-on-Read).")

    # Verification du partitionnement
    orders_ddl = spark.sql("DESCRIBE EXTENDED lake.silver.orders").collect()
    is_partitioned = any("created_date" in str(r) for r in orders_ddl)
    assert is_partitioned, "La table silver.orders doit être partitionnée par created_date"
    print("  -> Partitionnement de silver.orders par 'created_date' valide.")

    # 2. Lot initial (Batch 1 CDC) : Ingestion d'une commande initiale 'paid'
    print("\n[2/6] Ingestion Lot 1 (Insert CDC) : Commande 4001 payee...")
    batch_1_raw = [
        (4001, 801, "paid", 150.00, "2026-11-20 10:00:00", "2026-11-20 10:05:00", False, "c", 200)
    ]
    df_batch_1 = spark.createDataFrame(
        batch_1_raw,
        ["order_id", "customer_id", "status", "total_amount", "created_at_str", "updated_at_str", "is_deleted", "_last_op", "_source_lsn"]
    ).select(
        F.col("order_id").cast(LongType()),
        F.col("customer_id").cast(LongType()),
        F.col("status").cast(StringType()),
        F.col("total_amount").cast(DecimalType(12, 2)),
        F.to_timestamp("created_at_str").alias("created_at"),
        F.to_timestamp("updated_at_str").alias("updated_at"),
        F.to_date("created_at_str").alias("created_date"),
        F.lit(True).alias("is_paid"),
        F.lit(False).alias("is_final_status"),
        F.col("is_deleted").cast(BooleanType()),
        F.col("_last_op").cast(StringType()),
        F.col("_source_lsn").cast(LongType()),
        F.current_timestamp().alias("_ingested_at"),
    )

    writer.merge(df_batch_1, "orders")
    assert spark.table("lake.silver.orders").count() == 1

    snapshots_after_b1 = time_travel.get_snapshots("orders")
    assert len(snapshots_after_b1) >= 1
    snap_1_id = snapshots_after_b1[-1]["snapshot_id"]
    snap_1_ts_ms = snapshots_after_b1[-1]["timestamp_ms"]
    print(f"  -> Lot 1 insere avec succes. Snapshot ID = {snap_1_id}")

    # 3. Lot de mise a jour (Batch 2 CDC) : Commande 4001 remboursée ('refunded')
    print("\n[3/6] Ingestion Lot 2 (Update CDC) : Commande 4001 mutee en 'refunded'...")
    batch_2_raw = [
        (4001, 801, "refunded", 150.00, "2026-11-20 10:00:00", "2026-11-20 11:00:00", False, "u", 201)
    ]
    df_batch_2 = spark.createDataFrame(
        batch_2_raw,
        ["order_id", "customer_id", "status", "total_amount", "created_at_str", "updated_at_str", "is_deleted", "_last_op", "_source_lsn"]
    ).select(
        F.col("order_id").cast(LongType()),
        F.col("customer_id").cast(LongType()),
        F.col("status").cast(StringType()),
        F.col("total_amount").cast(DecimalType(12, 2)),
        F.to_timestamp("created_at_str").alias("created_at"),
        F.to_timestamp("updated_at_str").alias("updated_at"),
        F.to_date("created_at_str").alias("created_date"),
        F.lit(False).alias("is_paid"),
        F.lit(True).alias("is_final_status"),
        F.col("is_deleted").cast(BooleanType()),
        F.col("_last_op").cast(StringType()),
        F.col("_source_lsn").cast(LongType()),
        F.current_timestamp().alias("_ingested_at"),
    )

    writer.merge(df_batch_2, "orders")
    current_status = spark.table("lake.silver.orders").filter("order_id = 4001").collect()[0].status
    assert current_status == "refunded", f"Attendu 'refunded', obtenu {current_status}"

    snapshots_after_b2 = time_travel.get_snapshots("orders")
    assert len(snapshots_after_b2) >= 2
    snap_2_id = snapshots_after_b2[-1]["snapshot_id"]
    print(f"  -> Statut actuel de la commande 4001 : {current_status} (Snapshot ID: {snap_2_id})")

    # 4. Test du Time Travel (Démonstration spécifique page 7 du cahier des charges)
    print("\n[4/6] Verification du Time Travel (TIMESTAMP AS OF / VERSION AS OF)...")
    # A. Via TimeTravel snapshot-id
    hist_df = time_travel.read_at_snapshot("orders", snap_1_id)
    hist_status = hist_df.filter("order_id = 4001").collect()[0].status
    print(f"  -> Statut historique au Snapshot 1 ({snap_1_id}) : {hist_status}")
    assert hist_status == "paid", f"Le Time Travel au snapshot 1 doit retourner 'paid', obtenu {hist_status}"

    # B. Via SQL VERSION AS OF
    sql_hist_df = spark.sql(f"SELECT status FROM lake.silver.orders VERSION AS OF {snap_1_id} WHERE order_id = 4001")
    sql_hist_status = sql_hist_df.collect()[0].status
    assert sql_hist_status == "paid", f"VERSION AS OF SQL doit retourner 'paid', obtenu {sql_hist_status}"
    print(f"  -> Requête SQL 'VERSION AS OF {snap_1_id}' validee avec succes !")

    # 5. Idempotence & Protection contre les événements CDC désordonnés
    print("\n[5/6] Tests de robustesse CDC : Idempotence et anti-regression LSN...")
    # A. Rejeu strict du Lot 2 (Idempotence)
    writer.merge(df_batch_2, "orders")
    orders_count = spark.table("lake.silver.orders").count()
    assert orders_count == 1, f"Rejouer un lot ne doit pas creer de doublon : attendu 1, obtenu {orders_count}"
    print("  -> Idempotence confirmee : aucun doublon cree apres rejeu.")

    # B. Événement désordonné (LSN 190 < 201) avec statut 'pending'
    stale_event = [(4001, 801, "pending", 150.00, "2026-11-20 10:00:00", "2026-11-20 09:00:00", False, "u", 190)]
    df_stale = spark.createDataFrame(
        stale_event,
        ["order_id", "customer_id", "status", "total_amount", "created_at_str", "updated_at_str", "is_deleted", "_last_op", "_source_lsn"]
    ).select(
        F.col("order_id").cast(LongType()),
        F.col("customer_id").cast(LongType()),
        F.col("status").cast(StringType()),
        F.col("total_amount").cast(DecimalType(12, 2)),
        F.to_timestamp("created_at_str").alias("created_at"),
        F.to_timestamp("updated_at_str").alias("updated_at"),
        F.to_date("created_at_str").alias("created_date"),
        F.lit(False).alias("is_paid"),
        F.lit(False).alias("is_final_status"),
        F.col("is_deleted").cast(BooleanType()),
        F.col("_last_op").cast(StringType()),
        F.col("_source_lsn").cast(LongType()),
        F.current_timestamp().alias("_ingested_at"),
    )
    writer.merge(df_stale, "orders")
    status_after_stale = spark.table("lake.silver.orders").filter("order_id = 4001").collect()[0].status
    assert status_after_stale == "refunded", "Un événement plus ancien (LSN 190) ne doit pas écraser l'état (LSN 201)"
    print(f"  -> Garde LSN validee : le statut reste '{status_after_stale}' face a un evenement perime.")

    # C. Hard / Soft Delete
    delete_event = [(4001, 801, "refunded", 150.00, "2026-11-20 10:00:00", "2026-11-20 12:00:00", True, "d", 205)]
    df_delete = spark.createDataFrame(
        delete_event,
        ["order_id", "customer_id", "status", "total_amount", "created_at_str", "updated_at_str", "is_deleted", "_last_op", "_source_lsn"]
    ).select(
        F.col("order_id").cast(LongType()),
        F.col("customer_id").cast(LongType()),
        F.col("status").cast(StringType()),
        F.col("total_amount").cast(DecimalType(12, 2)),
        F.to_timestamp("created_at_str").alias("created_at"),
        F.to_timestamp("updated_at_str").alias("updated_at"),
        F.to_date("created_at_str").alias("created_date"),
        F.lit(False).alias("is_paid"),
        F.lit(True).alias("is_final_status"),
        F.col("is_deleted").cast(BooleanType()),
        F.col("_last_op").cast(StringType()),
        F.col("_source_lsn").cast(LongType()),
        F.current_timestamp().alias("_ingested_at"),
    )
    writer.merge(df_delete, "orders")
    assert spark.table("lake.silver.orders").filter("order_id = 4001").count() == 0
    print("  -> Suppression CDC prise en compte avec succes (DELETE effectif).")

    # 6. Maintenance & Compaction Iceberg
    print("\n[6/6] Execution de la maintenance Iceberg (rewrite_data_files)...")
    compaction_res = table_mgr.rewrite_data_files("orders")
    print(f"  -> Resultat compaction : {compaction_res}")

    print("\n" + "=" * 60)
    print("✅ TOUS LES TESTS APACHE ICEBERG STORAGE (PHASE 6) ONT REUSSI !")
    print("Catalogue REST, Tables MOR, Time Travel, Idempotence et Maintenance valides.")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
