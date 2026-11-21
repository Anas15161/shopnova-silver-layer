"""Test d'integration end-to-end pour le Data Quality Framework (Phase 5)."""

import sys
if "/home/iceberg" not in sys.path:
    sys.path.append("/home/iceberg")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from src.quality.engine import DataQualityEngine
from src.quality.quarantine import QuarantineManager


def main():
    print("=" * 60)
    print("DEMARRAGE DU TEST E2E DU DATA QUALITY FRAMEWORK (PHASE 5)")
    print("=" * 60)

    spark = SparkSession.builder.appName("ShopNova-DataQuality-Test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Chargement de la configuration déclarative
    engine = DataQualityEngine(
        rules_dir="/home/iceberg/configs/quality_rules",
        quarantine_manager=QuarantineManager("lake.quarantine"),
    )
    orders_quality_cfg = engine.load_config("orders")
    print(f"[1/4] Regles de qualite chargees: {len(orders_quality_cfg.rules)} regles declarees.")
    for r in orders_quality_cfg.rules:
        print(f"      - [{r.dimension.value.upper()}] {r.name} ({r.severity.value}) : {r.condition}")

    # 2. Construction d'un jeu de données synthétique contenant des anomalies ciblées
    print("\n[2/4] Creation d'un lot de test avec injections d'anomalies...")
    # Schéma Silver
    schema = StructType([
        StructField("order_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("status", StringType(), True),
        StructField("total_amount", DecimalType(12, 2), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("is_deleted", BooleanType(), True),
        StructField("_last_op", StringType(), True),
        StructField("_source_lsn", LongType(), True),
        StructField("_ingested_at", TimestampType(), True),
    ])

    test_data = [
        # Ligne 1 : Parfaitement valide (PAID)
        (3001, 701, "paid", 150.00, "2026-11-20 10:00:00", "2026-11-20 10:05:00", False, "c", 101, "2026-11-20 10:05:00"),
        # Ligne 2 : Parfaitement valide (SHIPPED)
        (3002, 702, "shipped", 89.90, "2026-11-20 10:30:00", "2026-11-20 12:00:00", False, "u", 102, "2026-11-20 12:00:00"),
        # Ligne 3 : Parfaitement valide (PENDING)
        (3003, 703, "pending", 25.00, "2026-11-20 11:00:00", "2026-11-20 11:00:00", False, "c", 103, "2026-11-20 11:00:00"),
        # Ligne 4 : ANOMALIE 1 - Montant négatif (-99.00) sur commande non supprimée
        (3004, 704, "paid", -99.00, "2026-11-20 11:15:00", "2026-11-20 11:15:00", False, "c", 104, "2026-11-20 11:15:00"),
        # Ligne 5 : ANOMALIE 2 - Statut invalide ('corrupted_status')
        (3005, 705, "corrupted_status", 45.00, "2026-11-20 11:30:00", "2026-11-20 11:30:00", False, "c", 105, "2026-11-20 11:30:00"),
        # Ligne 6 : ANOMALIE 3 - Incohérence temporelle (updated_at antérieur à created_at)
        (3006, 706, "paid", 200.00, "2026-11-20 12:00:00", "2026-11-19 12:00:00", False, "c", 106, "2026-11-20 12:00:00"),
    ]

    from pyspark.sql import functions as F
    df_raw = spark.createDataFrame(
        [(d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9]) for d in test_data],
        ["order_id", "customer_id", "status", "total_amount", "created_at_str", "updated_at_str", "is_deleted", "_last_op", "_source_lsn", "_ingested_at_str"]
    ).select(
        F.col("order_id").cast(LongType()),
        F.col("customer_id").cast(LongType()),
        F.col("status"),
        F.col("total_amount").cast(DecimalType(12, 2)),
        F.to_timestamp("created_at_str").alias("created_at"),
        F.to_timestamp("updated_at_str").alias("updated_at"),
        F.col("is_deleted").cast(BooleanType()),
        F.col("_last_op"),
        F.col("_source_lsn").cast(LongType()),
        F.to_timestamp("_ingested_at_str").alias("_ingested_at")
    )

    print(f"  -> Total enregistrements à analyser : {df_raw.count()}")

    # 3. Évaluation de la qualité et dérivation en quarantaine
    print("\n[3/4] Evaluation des regles et routage vers la Quarantaine...")
    valid_df, quarantined_df, report = engine.evaluate(df_raw, orders_quality_cfg, auto_quarantine=True)

    print("\n--- RAPPORT QUALITE SHOPNOVA ---")
    print(f"Table                  : {report.table_name}")
    print(f"Total Enregistrements  : {report.total_records}")
    print(f"Enregistrements Valides: {report.valid_records}")
    print(f"En Quarantaine         : {report.quarantined_records}")
    print(f"Taux de Succes         : {report.pass_rate} %")
    print("---------------------------------")
    for r in report.rules_summary:
        status_icon = "✅" if r.is_success else "❌"
        print(f"  {status_icon} [{r.dimension.upper()}] {r.rule_name:<30} : {r.passed_count} valides / {r.failed_count} echecs")

    # Assertions strictes sur le rapport
    assert report.total_records == 6, f"Attendu 6 au total, obtenu {report.total_records}"
    assert report.valid_records == 3, f"Attendu 3 valides, obtenu {report.valid_records}"
    assert report.quarantined_records == 3, f"Attendu 3 rejets, obtenu {report.quarantined_records}"
    assert report.pass_rate == 50.0, f"Attendu 50% de succès, obtenu {report.pass_rate}"

    # 4. Vérification du contenu de la quarantaine
    print("\n[4/4] Verification des enregistrements en Quarantaine...")
    print("Enregistrements rejetés avec motifs :")
    quarantined_df.select("order_id", "status", "total_amount", "_rejection_reason", "_rejected_at").show(truncate=False)

    rejections = {row.order_id: row._rejection_reason for row in quarantined_df.collect()}
    assert "positive_total_amount" in rejections[3004], "La commande 3004 doit échouer pour montant négatif"
    assert "valid_status" in rejections[3005], "La commande 3005 doit échouer pour statut invalide"
    assert "chronological_consistency" in rejections[3006], "La commande 3006 doit échouer pour incohérence temporelle"

    # Vérification que la table Iceberg lake.quarantine stocke bien les rejets
    print("Lecture directe depuis le catalogue Iceberg lake.quarantine.orders...")
    quarantine_stored_df = spark.table("lake.quarantine.orders")
    assert quarantine_stored_df.count() >= 3, "Les donnees de quarantaine doivent persister dans Iceberg"
    print(f"  -> {quarantine_stored_df.count()} lignes persistees avec succes dans 'lake.quarantine.orders'.")

    print("\n" + "=" * 60)
    print("✅ TOUS LES TESTS DU DATA QUALITY FRAMEWORK ONT REUSSI !")
    print("5 dimensions évaluées, calcul des métriques et quarantaine MinIO validés.")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
