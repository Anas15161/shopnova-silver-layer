"""Converts declarative YAML string types to PySpark DataType instances."""

import re
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
)


class UnsupportedTypeException(ValueError):
    """Raised when a YAML schema defines an unrecognized data type."""

    pass


def parse_spark_type(type_str: str) -> DataType:
    """Parses a string type definition into a concrete PySpark DataType.

    Supported types:
        - string, text, varchar
        - bigint, long, int64
        - int, integer, int32
        - short, smallint, int16
        - boolean, bool
        - double, float64
        - float, float32
        - date
        - timestamp, datetime
        - decimal(precision, scale)
    """
    clean_type = type_str.strip().lower()

    if clean_type in ("string", "text", "varchar"):
        return StringType()
    if clean_type in ("bigint", "long", "int64"):
        return LongType()
    if clean_type in ("int", "integer", "int32"):
        return IntegerType()
    if clean_type in ("short", "smallint", "int16"):
        return ShortType()
    if clean_type in ("boolean", "bool"):
        return BooleanType()
    if clean_type in ("double", "float64"):
        return DoubleType()
    if clean_type in ("float", "float32"):
        return FloatType()
    if clean_type == "date":
        return DateType()
    if clean_type in ("timestamp", "datetime"):
        return TimestampType()

    # Decimal(precision, scale) regex parsing
    decimal_match = re.match(r"^decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$", clean_type)
    if decimal_match:
        precision = int(decimal_match.group(1))
        scale = int(decimal_match.group(2))
        return DecimalType(precision, scale)

    raise UnsupportedTypeException(
        f"Type de donnee non supporte: '{type_str}'. "
        "Types acceptes: string, bigint, int, decimal(p,s), timestamp, boolean, double, date."
    )
