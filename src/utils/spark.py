"""Spark session management utility."""

import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "ShopNova-Silver-Engine") -> SparkSession:
    """Returns an active SparkSession or builds a standard one."""
    master = os.getenv("SPARK_MASTER_URL", "local[*]")
    return SparkSession.builder.appName(app_name).master(master).getOrCreate()
