"""Tests unitaires pour les modèles et configurations du Data Quality Framework."""

from src.quality.engine import DataQualityEngine
from src.quality.models import (
    DimensionEnum,
    QualityConfig,
    QualityReport,
    QualityRule,
    RuleResult,
    SeverityEnum,
)


def test_quality_rule_validation():
    rule = QualityRule(
        name="test_rule",
        dimension=DimensionEnum.COMPLETENESS,
        condition="id IS NOT NULL",
        severity=SeverityEnum.ERROR,
    )
    assert rule.name == "test_rule"
    assert rule.dimension == DimensionEnum.COMPLETENESS
    assert rule.severity == SeverityEnum.ERROR


def test_quality_report_metrics():
    summary = [
        RuleResult(
            rule_name="col_not_null",
            dimension="completeness",
            severity="ERROR",
            passed_count=90,
            failed_count=10,
            is_success=False,
        )
    ]
    report = QualityReport(
        table_name="orders",
        total_records=100,
        valid_records=90,
        quarantined_records=10,
        pass_rate=90.0,
        rules_summary=summary,
    )
    report_dict = report.to_dict()
    assert report_dict["table_name"] == "orders"
    assert report_dict["pass_rate"] == 90.0
    assert report_dict["quarantined_records"] == 10
    assert len(report_dict["rules_summary"]) == 1


def test_load_orders_quality_yaml(configs_dir):
    engine = DataQualityEngine(rules_dir=configs_dir / "quality_rules")
    config = engine.load_config("orders")
    assert isinstance(config, QualityConfig)
    assert config.table_name == "orders"
    assert len(config.rules) >= 5
    rule_names = [r.name for r in config.rules]
    assert "order_id_not_null" in rule_names
    assert "valid_status" in rule_names
    assert "chronological_consistency" in rule_names
