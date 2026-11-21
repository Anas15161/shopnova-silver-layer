"""Transformer implementation for the Orders dataset."""

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.transformers.base import BaseTransformer


class OrdersTransformer(BaseTransformer):
    """Encapsulates business rules, deduplication, and enrichment for Orders."""

    ALLOWED_STATUSES: List[str] = [
        "pending",
        "paid",
        "shipped",
        "delivered",
        "cancelled",
        "refunded",
    ]

    def transform(self, df: DataFrame) -> DataFrame:
        """Executes full transformation on canonical orders DataFrame."""
        # 1. Null handling sur identifiants obligatoires
        cleaned = df.filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())

        # 2. Trim de toutes les chaînes de caractères
        cleaned = self.trim_all_strings(cleaned)

        # 3. Déduplication par commande basée sur le dernier LSN (Règle d'or CDC)
        deduped = self.deduplicate_by_lsn(cleaned, primary_key="order_id", lsn_col="_source_lsn")

        # 4. Standardisation des statuts (mise en minuscules)
        standardized = deduped.withColumn("status", F.lower(F.col("status")))

        # 5. Validation métier : montant positif ou null autorisé uniquement si suppression
        valid = standardized.filter(
            (F.col("is_deleted") == True)  # noqa: E712
            | (F.col("total_amount").isNotNull() & (F.col("total_amount") >= 0))
        )

        # 6. Enrichissement métier
        enriched = (
            valid
            # Date de partitionnement Iceberg
            .withColumn("created_date", F.to_date(F.col("created_at")))
            # Indicateurs de statut
            .withColumn("is_paid", F.col("status") == "paid").withColumn(
                "is_final_status",
                F.col("status").isin("shipped", "delivered", "cancelled", "refunded"),
            )
        )

        return enriched
