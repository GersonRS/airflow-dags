from __future__ import annotations

import json
from typing import Any

from pyspark.sql.dataframe import DataFrame


def generate_avro_schema_from_json(json_data: dict[str, str]) -> dict[str, Any]:
    def avro_type_mapping(value: str) -> list[str]:
        if value is None:
            return ["null", "string"]
        elif isinstance(value, bool):
            return ["null", "boolean"]
        elif isinstance(value, int):
            return ["null", "int"]
        elif isinstance(value, float):
            return ["null", "double"]
        elif isinstance(value, str):
            return ["null", "string"]
        else:
            return ["null", "string"]

    avro_schema = {
        "type": "record",
        "name": "Default_schema",
        "fields": [
            {"name": key, "type": avro_type_mapping(value)} for key, value in json_data.items()
        ],
    }
    return avro_schema


def get_avro_schema(spark_df: DataFrame, schema_type: str, name: str, namespace: str) -> str:
    """
    Returns the corresponding avro schema for the passed in spark dataframe.
    The type mapping covers most commonly used types, every field is made to be nullable.
    """

    schema_base: dict[str, Any] = {
        "type": schema_type,
        "namespace": name,
        "name": namespace,
    }

    # Keys are Spark Types, Values are Avro Types
    avro_mapping = {
        "StringType": ["string", "null"],
        "LongType": ["long", "null"],
        "IntegerType": ["int", "null"],
        "BooleanType": ["boolean", "null"],
        "FloatType": ["float", "null"],
        "DoubleType": ["double", "null"],
        "TimestampType": ["long", "null"],
        "ArrayType(StringType,true)": [
            {"type": "array", "items": ["string", "null"]},
            "null",
        ],
        "ArrayType(IntegerType,true)": [
            {"type": "array", "items": ["int", "null"]},
            "null",
        ],
    }

    fields = []

    for field in spark_df.schema.fields:
        if str(field.dataType) in avro_mapping:
            fields.append({"name": field.name, "type": avro_mapping[str(field.dataType)]})
        else:
            fields.append({"name": field.name, "type": str(field.dataType)})

    schema_base["fields"] = fields

    return json.dumps(schema_base)
