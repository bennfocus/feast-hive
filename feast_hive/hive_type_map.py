from typing import Dict

import pyarrow as pa

from feast import ValueType


def hive_to_feast_value_type(hive_type_as_str: str) -> ValueType:
    type_map: Dict[str, ValueType] = {
        "tinyint": ValueType.INT32,
        "smallint": ValueType.INT32,
        "int": ValueType.INT32,
        "integer": ValueType.INT32,
        "bigint": ValueType.INT64,
        "float": ValueType.FLOAT,
        "double": ValueType.DOUBLE,
        "numeric": ValueType.DOUBLE,
        "timestamp": ValueType.UNIX_TIMESTAMP,
        "string": ValueType.STRING,
        "varchar": ValueType.STRING,
        "char": ValueType.STRING,
        "boolean": ValueType.BOOL,
    }
    return type_map[hive_type_as_str.lower()]


def pa_to_hive_value_type(pa_type_as_str: str) -> str:
    # PyArrow types: https://arrow.apache.org/docs/python/api/datatypes.html
    # Hive type: https://cwiki.apache.org/confluence/display/hive/languagemanual+types
    pa_type_as_str = pa_type_as_str.lower()
    if pa_type_as_str.startswith("timestamp"):
        if "tz=" in pa_type_as_str:
            return "timestamp"  # Hive does not support timestamp with timezone.
        else:
            return "timestamp"

    if pa_type_as_str.startswith("date"):
        return "date"

    if pa_type_as_str.startswith("decimal"):
        return pa_type_as_str

    if pa_type_as_str.startswith("dictionary<values=string,"):
        return "string"

    type_map = {
        "null": "null",
        "bool": "boolean",
        "int8": "tinyint",
        "int16": "smallint",
        "int32": "int",
        "int64": "bigint",
        "uint8": "smallint",
        "uint16": "int",
        "uint32": "bigint",
        "uint64": "decimal",
        "float": "float",
        "double": "double",
        "binary": "binary",
        "string": "string",
    }
    return type_map[pa_type_as_str]


_HIVE_TO_PA_TYPE_MAP = {
    "null": pa.null(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us"),
    "date": pa.date32(),
    "tinyint": pa.int8(),
    "smallint": pa.int16(),
    "int": pa.int32(),
    "bigint": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "binary": pa.binary(),
    "string": pa.string(),
    "varchar": pa.string(),
}


def hive_to_pa_value_type(hive_type_as_str: str) -> pa.DataType:
    hive_type_as_str = hive_type_as_str.lower()
    # if hive_type_as_str.startswith("decimal"):
    #     return pa.decimal256()

    return _HIVE_TO_PA_TYPE_MAP[hive_type_as_str]
