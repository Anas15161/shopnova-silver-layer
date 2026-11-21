"""Transformer implementation for the Products dataset."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.transformers.base import BaseTransformer


class ProductsTransformer(BaseTransformer):
    """Encapsulates business rules, deduplication, and enrichment for Products."""

    def transform(self, df: DataFrame) -> DataFrame:
        # 1. Null handling sur identifiant
        cleaned = df.filter(F.col("product_id").isNotNull())

        # 2. Trim des chaînes
        cleaned = self.trim_all_strings(cleaned)

        # 3. Déduplication par product_id basée sur le dernier LSN
        deduped = self.deduplicate_by_lsn(cleaned, primary_key="product_id", lsn_col="_source_lsn")

        # 4. Standardisation des catégories et noms
        standardized = deduped.withColumn("category", F.lower(F.col("category")))

        # 5. Validation métier : prix et stock positifs ou nuls
        valid = standardized.filter(
            (F.col("is_deleted") == True)  # noqa: E712
            | ((F.col("price") >= 0) & (F.col("stock_quantity") >= 0))
        )

        # 6. Enrichissement métier
        enriched = valid.withColumn("is_in_stock", F.col("stock_quantity") > 0)

        return enriched
