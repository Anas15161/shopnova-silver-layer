"""Test d'intégration vérifiant l'infrastructure Docker active."""

import json
import urllib.request
import boto3
from botocore.client import Config


def test_minio_health():
    """Vérifie que l'API MinIO est accessible et saine."""
    req = urllib.request.Request("http://localhost:9000/minio/health/live")
    with urllib.request.urlopen(req, timeout=5) as response:
        assert response.status == 200


def test_iceberg_rest_catalog_config():
    """Vérifie que le catalogue Apache Iceberg REST répond sur le port 8181."""
    req = urllib.request.Request("http://localhost:8181/v1/config")
    with urllib.request.urlopen(req, timeout=5) as response:
        assert response.status == 200
        data = json.loads(response.read().decode())
        assert isinstance(data, dict)


def test_minio_buckets_exist():
    """Vérifie que les buckets requis ont bien été initialisés par mc-init."""
    s3_client = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    response = s3_client.list_buckets()
    bucket_names = [b["Name"] for b in response.get("Buckets", [])]

    required_buckets = ["bronze", "silver", "quarantine", "warehouse"]
    for bucket in required_buckets:
        assert bucket in bucket_names, f"Bucket requis '{bucket}' non trouvé dans MinIO."
