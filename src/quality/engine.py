"""Data Quality Evaluation Engine for PySpark."""

from pathlib import Path
from typing import List, Optional, Tuple, Union
import yaml
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from src.quality.models import (
    QualityConfig,
    QualityReport,
    RuleResult,
    SeverityEnum,
)
from src.quality.quarantine import QuarantineManager


class DataQualityEngine:
    """Evaluates declarative quality rules and separates valid records from quarantine."""

    def __init__(
        self,
        rules_dir: Optional[Union[str, Path]] = None,
        quarantine_manager: Optional[QuarantineManager] = None,
    ):
        if rules_dir is None:
            self.rules_dir = (
                Path(__file__).resolve().parent.parent.parent / "configs" / "quality_rules"
            )
        else:
            self.rules_dir = Path(rules_dir)

        self.quarantine_manager = quarantine_manager or QuarantineManager()

    def load_config(self, table_name_or_file: str) -> QualityConfig:
        """Loads QualityConfig from YAML."""
        filename = (
            table_name_or_file
            if table_name_or_file.endswith(".yaml")
            else f"{table_name_or_file}_quality.yaml"
        )
        file_path = self.rules_dir / filename

        if not file_path.exists():
            raise FileNotFoundError(f"Fichier de regles introuvable: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return QualityConfig(**data)

    def evaluate(
        self, df: DataFrame, config: QualityConfig, auto_quarantine: bool = True
    ) -> Tuple[DataFrame, DataFrame, QualityReport]:
        """Evaluates all rules, splits data, optionally saves to quarantine, and builds a report.

        Returns:
            Tuple of (valid_df, quarantined_df, report).
        """
        total_count = df.count()
        if total_count == 0:
            report = QualityReport(
                table_name=config.table_name,
                total_records=0,
                valid_records=0,
                quarantined_records=0,
                pass_rate=100.0,
            )
            empty_quarantine = df.withColumn("_rejection_reasons", F.array())
            return df, empty_quarantine, report

        rule_results: List[RuleResult] = []
        failure_exprs: List[Column] = []

        # 1. Evaluate each rule condition
        for rule in config.rules:
            cond = F.expr(rule.condition)
            passed_count = df.filter(cond).count()
            failed_count = total_count - passed_count
            is_success = failed_count == 0

            rule_results.append(
                RuleResult(
                    rule_name=rule.name,
                    dimension=rule.dimension.value,
                    severity=rule.severity.value,
                    passed_count=passed_count,
                    failed_count=failed_count,
                    is_success=is_success,
                )
            )

            # If severity is ERROR, flag row for quarantine
            if rule.severity == SeverityEnum.ERROR:
                failure_exprs.append(
                    F.when(~cond, F.lit(f"{rule.dimension.value}:{rule.name}")).otherwise(
                        F.lit(None)
                    )
                )

        # 2. Build array of failed reasons for each row
        if failure_exprs:
            tagged_df = (
                df.withColumn("_raw_reasons", F.array(*failure_exprs))
                .withColumn(
                    "_rejection_reasons", F.expr("filter(_raw_reasons, x -> x IS NOT NULL)")
                )
                .drop("_raw_reasons")
            )
        else:
            tagged_df = df.withColumn("_rejection_reasons", F.array())

        # 3. Split valid rows and quarantined rows
        is_quarantined_cond = F.size(F.col("_rejection_reasons")) > 0

        quarantined_df = (
            tagged_df.filter(is_quarantined_cond)
            .withColumn("_rejection_reason", F.concat_ws("; ", F.col("_rejection_reasons")))
            .withColumn("_rejected_at", F.current_timestamp())
            .drop("_rejection_reasons")
        )

        valid_df = tagged_df.filter(~is_quarantined_cond).drop("_rejection_reasons")

        quarantined_count = quarantined_df.count()
        valid_count = total_count - quarantined_count
        pass_rate = round((valid_count / total_count) * 100.0, 2) if total_count > 0 else 100.0

        # 4. Save to quarantine storage if configured and rejections exist
        if auto_quarantine and quarantined_count > 0:
            self.quarantine_manager.save_to_quarantine(quarantined_df, config.table_name)

        # 5. Build consolidated report
        report = QualityReport(
            table_name=config.table_name,
            total_records=total_count,
            valid_records=valid_count,
            quarantined_records=quarantined_count,
            pass_rate=pass_rate,
            rules_summary=rule_results,
        )

        return valid_df, quarantined_df, report
