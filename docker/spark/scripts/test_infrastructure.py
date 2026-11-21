"""Test de validation de l'infrastructure Spark + Iceberg REST + MinIO S3."""

import sys
from pyspark.sql import SparkSession


def main():
    print("=" * 60)
    print("DEMARRAGE DU TEST D'INFRASTRUCTURE SHOPNOVA")
    print("=" * 60)

    # 1. Initialisation de la SparkSession
    spark = SparkSession.builder.appName("ShopNova-Infra-Test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("[1/5] SparkSession initialisée avec succès.")

    # 2. Création du namespace dans le catalogue Iceberg
    print("[2/5] Création du namespace test dans le catalogue REST 'lake'...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.infra_check")

    # 3. Création de la table Iceberg de test
    print("[3/5] Création de la table Iceberg lake.infra_check.healthcheck...")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lake.infra_check.healthcheck (
            check_id INT,
            service_name STRING,
            status STRING,
            checked_at TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES ('format-version'='2')
    """
    )

    # 4. Écriture dans MinIO S3 via Iceberg
    print("[4/5] Insertion de données de test (test d'écriture S3 / MinIO)...")
    spark.sql(
        """
        INSERT INTO lake.infra_check.healthcheck
        VALUES (1, 'MinIO + Iceberg REST + Spark', 'HEALTHY', current_timestamp())
    """
    )

    # 5. Lecture et vérification
    print("[5/5] Relecture des données depuis le catalogue Iceberg...")
    df = spark.sql("SELECT * FROM lake.infra_check.healthcheck")
    df.show(truncate=False)

    count = df.count()
    assert count == 1, f"Attendu 1 enregistrement, obtenu {count}"

    # Nettoyage
    spark.sql("DROP TABLE lake.infra_check.healthcheck")
    spark.sql("DROP NAMESPACE lake.infra_check")

    print("=" * 60)
    print("✅ TEST D'INFRASTRUCTURE REUSSI AVEC SUCCES !")
    print("MinIO S3, Iceberg REST Catalog et Spark fonctionnent de concert.")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
