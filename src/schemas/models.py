"""Pydantic models for declarative schema configurations."""

from typing import List, Optional, Union
from pydantic import BaseModel, Field, field_validator


class ColumnDefinition(BaseModel):
    """Specification of a single Silver target column."""

    name: str = Field(..., description="Target Silver column name")
    type: str = Field(
        ..., description="Target data type (string, bigint, decimal(p,s), timestamp, boolean)"
    )
    required: bool = Field(default=True, description="Whether the column is mandatory")
    source_fields: List[str] = Field(
        default_factory=list,
        description="Candidate source fields in priority order",
    )
    description: Optional[str] = Field(default=None, description="Column documentation")

    @field_validator("source_fields", mode="before")
    @classmethod
    def coerce_source_fields(cls, v: Union[str, List[str]]) -> List[str]:
        if isinstance(v, str):
            return [v]
        return v or []


class CDCMetadataConfig(BaseModel):
    """Configuration for Change Data Capture (Debezium) envelope mapping."""

    enabled: bool = Field(default=True)
    op_field: str = Field(default="op")
    lsn_field: str = Field(default="source.lsn")
    ts_ms_field: str = Field(default="ts_ms")


class TableSchemaConfig(BaseModel):
    """Canonical schema configuration for a Silver table."""

    table_name: str = Field(..., description="Target table name (e.g. orders)")
    version: str = Field(default="1.0.0", description="Contract version")
    primary_key: str = Field(..., description="Primary key column name")
    partition_column: Optional[str] = Field(default=None, description="Partitioning column")
    partition_granularity: Optional[str] = Field(
        default="days", description="Partitioning granularity (days, months)"
    )
    allow_schema_evolution: bool = Field(
        default=False, description="Whether to allow undeclared columns"
    )
    columns: List[ColumnDefinition] = Field(..., description="List of canonical column definitions")
    cdc_metadata: Optional[CDCMetadataConfig] = Field(default_factory=CDCMetadataConfig)

    def get_column(self, name: str) -> Optional[ColumnDefinition]:
        """Finds a column definition by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    @property
    def mandatory_column_names(self) -> List[str]:
        """Returns the list of mandatory column names."""
        return [c.name for c in self.columns if c.required]
