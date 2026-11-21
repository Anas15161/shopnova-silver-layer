"""Schema management engine: mapping, casting, validation, and CDC enrichment."""

from pathlib import Path
from typing import Dict, List, Optional, Union
import yaml
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from src.schemas.models import TableSchemaConfig
from src.schemas.type_converter import parse_spark_type


class SchemaValidationError(ValueError):
    """Raised when an incoming dataset fails schema contract requirements."""

    pass


class SchemaManager:
    """Loads declarative YAML contracts and validates/transforms PySpark DataFrames."""

    def __init__(self, schemas_dir: Optional[Union[str, Path]] = None):
        if schemas_dir is None:
            self.schemas_dir = Path(__file__).resolve().parent.parent.parent / "configs" / "schemas"
        else:
            self.schemas_dir = Path(schemas_dir)

    def load_schema(self, table_name_or_file: str) -> TableSchemaConfig:
        """Loads a TableSchemaConfig from a YAML file name or table identifier."""
        filename = (
            table_name_or_file
            if table_name_or_file.endswith(".yaml")
            else f"{table_name_or_file}.yaml"
        )
        file_path = self.schemas_dir / filename

        if not file_path.exists():
            raise FileNotFoundError(f"Fichier de schema introuvable: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return TableSchemaConfig(**data)

    def _resolve_source_column(self, df: DataFrame, candidates: List[str]) -> Optional[str]:
        """Finds the first candidate column or nested path existing in the DataFrame."""
        flat_columns = df.columns
        for candidate in candidates:
            if "." in candidate:
                root = candidate.split(".")[0]
                if root in flat_columns:
                    return candidate
            else:
                if candidate in flat_columns:
                    return candidate
        return None

    def validate_and_transform(self, df: DataFrame, schema_config: TableSchemaConfig) -> DataFrame:
        """Validates incoming DataFrame against schema contract and transforms to Silver schema."""
        missing_mandatory = []
        col_exprs: Dict[str, Column] = {}

        # 1. Resolve domain columns
        for col_def in schema_config.columns:
            target_name = col_def.name
            target_type = parse_spark_type(col_def.type)

            candidates = list(col_def.source_fields)
            if target_name not in candidates:
                candidates.append(target_name)

            resolved_path = self._resolve_source_column(df, candidates)

            if resolved_path is None:
                if col_def.required:
                    missing_mandatory.append(f"{target_name} (candidats cherches: {candidates})")
                else:
                    col_exprs[target_name] = F.lit(None).cast(target_type)
            else:
                raw_col = F.col(resolved_path)
                if isinstance(target_type, TimestampType):
                    col_exprs[target_name] = F.coalesce(
                        F.to_timestamp(raw_col),
                        F.to_timestamp(raw_col, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                        F.to_timestamp(raw_col, "yyyy-MM-dd HH:mm:ss"),
                    ).cast(target_type)
                else:
                    col_exprs[target_name] = raw_col.cast(target_type)

        if missing_mandatory:
            raise SchemaValidationError(
                f"Contrat de schema viole pour '{schema_config.table_name}'. "
                f"Colonnes obligatoires manquantes: {', '.join(missing_mandatory)}"
            )

        # 2. Build full canonical projection list
        projected_cols = [col_exprs[c.name].alias(c.name) for c in schema_config.columns]

        # 3. Add CDC metadata from raw df
        cdc_cfg = schema_config.cdc_metadata
        if cdc_cfg and cdc_cfg.enabled:
            # is_deleted
            if cdc_cfg.op_field in df.columns:
                projected_cols.append((F.col(cdc_cfg.op_field) == "d").alias("is_deleted"))
                projected_cols.append(F.col(cdc_cfg.op_field).cast("string").alias("_last_op"))
            else:
                projected_cols.append(F.lit(False).alias("is_deleted"))
                projected_cols.append(F.lit("r").alias("_last_op"))

            # _source_lsn
            lsn_path = self._resolve_source_column(df, [cdc_cfg.lsn_field, "lsn", "_source_lsn"])
            if lsn_path:
                projected_cols.append(F.col(lsn_path).cast("bigint").alias("_source_lsn"))
            else:
                projected_cols.append(F.lit(0).cast("bigint").alias("_source_lsn"))

        # 4. Add ingestion audit timestamp
        projected_cols.append(F.current_timestamp().alias("_ingested_at"))

        return df.select(*projected_cols)
