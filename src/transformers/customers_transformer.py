"""Transformer implementation for the Customers dataset."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.transformers.base import BaseTransformer


class CustomersTransformer(BaseTransformer):
    """Encapsulates business rules, deduplication, and enrichment for Customers."""

    def transform(self, df: DataFrame) -> DataFrame:
        # 1. Null handling sur identifiant
        cleaned = df.filter(F.col("customer_id").isNotNull())

        # 2. Trim de toutes les chaînes
        cleaned = self.trim_all_strings(cleaned)

        # 3. Déduplication par customer_id basée sur le dernier LSN
        deduped = self.deduplicate_by_lsn(cleaned, primary_key="customer_id", lsn_col="_source_lsn")

        # 4. Standardisation des noms et de l'email
        standardized = (
            deduped.withColumn("first_name", F.initcap(F.col("first_name")))
            .withColumn("last_name", F.initcap(F.col("last_name")))
            .withColumn("email", F.lower(F.col("email")))
            .withColumn("phone", F.coalesce(F.col("phone"), F.lit("UNKNOWN")))
            .withColumn("city", F.coalesce(F.initcap(F.col("city")), F.lit("UNKNOWN")))
        )

        # 5. Enrichissement : nom complet
        enriched = standardized.withColumn(
            "full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
        )

        return enriched
