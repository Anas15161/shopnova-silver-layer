"""Iceberg Time Travel and snapshot reader."""

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from pyiceberg.catalog import load_catalog
from pyspark.sql import DataFrame, SparkSession


class IcebergTimeTravel:
    """Provides historical snapshot inspection and Time Travel queries."""

    def __init__(
        self,
        spark: SparkSession,
        catalog: Optional[str] = None,
        namespace: Optional[str] = None,
        rest_uri: Optional[str] = None,
    ):
        self.spark = spark
        self.catalog = catalog or os.getenv("ICEBERG_CATALOG", "lake")
        self.namespace = namespace or os.getenv("SILVER_NAMESPACE", "silver")
        self.rest_uri = rest_uri or os.getenv("ICEBERG_REST_URI", "http://rest:8181")

        # PyIceberg client configuration for metadata inspection
        self._pyiceberg_catalog = None

    @property
    def full_namespace(self) -> str:
        return f"{self.catalog}.{self.namespace}"

    def _get_pyiceberg_catalog(self):
        if self._pyiceberg_catalog is None:
            endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
            access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
            secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
            region = os.getenv("AWS_REGION", "us-east-1")

            self._pyiceberg_catalog = load_catalog(
                self.catalog,
                **{
                    "uri": self.rest_uri,
                    "s3.endpoint": endpoint,
                    "s3.access-key-id": access_key,
                    "s3.secret-access-key": secret_key,
                    "s3.region": region,
                },
            )
        return self._pyiceberg_catalog

    def get_snapshots(self, table_name: str) -> List[Dict[str, Any]]:
        """Retrieves the list of committed snapshots for a given Silver table."""
        cat = self._get_pyiceberg_catalog()
        tbl = cat.load_table(f"{self.namespace}.{table_name}")

        results = []
        for snap in tbl.snapshots():
            results.append(
                {
                    "snapshot_id": snap.snapshot_id,
                    "parent_snapshot_id": snap.parent_snapshot_id,
                    "sequence_number": snap.sequence_number,
                    "timestamp_ms": snap.timestamp_ms,
                    "operation": snap.summary.operation.value if snap.summary else None,
                    "summary": dict(snap.summary.additional_properties) if snap.summary else {},
                }
            )
        return results

    def get_current_snapshot_id(self, table_name: str) -> Optional[int]:
        """Returns the ID of the current active snapshot."""
        snapshots = self.get_snapshots(table_name)
        if snapshots:
            return snapshots[-1]["snapshot_id"]
        return None

    def read_at_snapshot(self, table_name: str, snapshot_id: int) -> DataFrame:
        """Reads historical table state at a specific snapshot ID."""
        full_table = f"{self.full_namespace}.{table_name}"
        return self.spark.read.option("snapshot-id", snapshot_id).table(full_table)

    def read_at_timestamp(self, table_name: str, timestamp: Union[int, str, datetime]) -> DataFrame:
        """Reads historical table state at a specific point in time (as-of-timestamp)."""
        full_table = f"{self.full_namespace}.{table_name}"

        if isinstance(timestamp, int):
            ts_ms = timestamp
        elif isinstance(timestamp, datetime):
            ts_ms = int(timestamp.timestamp() * 1000)
        elif isinstance(timestamp, str):
            # Parse ISO timestamp
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ts_ms = int(dt.timestamp() * 1000)
        else:
            raise ValueError(f"Type de timestamp non supporté: {type(timestamp)}")

        return self.spark.read.option("as-of-timestamp", ts_ms).table(full_table)
