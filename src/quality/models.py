"""Pydantic models for the Data Quality Framework."""

from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class DimensionEnum(str, Enum):
    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    ACCURACY = "accuracy"


class SeverityEnum(str, Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class QualityRule(BaseModel):
    """Specification of a single quality verification rule."""

    name: str = Field(..., description="Unique rule name")
    dimension: DimensionEnum = Field(..., description="Quality dimension")
    condition: str = Field(
        ..., description="PySpark SQL boolean expression that valid rows satisfy"
    )
    severity: SeverityEnum = Field(
        default=SeverityEnum.ERROR, description="ERROR routes to quarantine"
    )
    description: Optional[str] = Field(default=None, description="Human readable rule purpose")


class QualityConfig(BaseModel):
    """Complete quality rulebook for a dataset."""

    table_name: str = Field(..., description="Target dataset name")
    version: str = Field(default="1.0.0")
    rules: List[QualityRule] = Field(default_factory=list)


class RuleResult(BaseModel):
    """Execution metrics for an individual rule."""

    rule_name: str
    dimension: str
    severity: str
    failed_count: int
    passed_count: int
    is_success: bool


class QualityReport(BaseModel):
    """Consolidated audit report produced after data quality evaluation."""

    table_name: str
    total_records: int
    valid_records: int
    quarantined_records: int
    pass_rate: float
    evaluated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    rules_summary: List[RuleResult] = Field(default_factory=list)

    def to_dict(self) -> Dict:
        return self.model_dump()
