from typing import Dict

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


def hive_to_pa_value_type(hive_type_as_str: str) -> str:
    hive_type_as_str = hive_type_as_str.lower()
    if hive_type_as_str.startswith("decimal"):
        return hive_type_as_str

    type_map = {
        "null": "null",
        "boolean": "bool",
        "timestamp": "timestamp[us]",
        "date": "date",
        "tinyint": "int8",
        "smallint": "int16",
        "int": "int32",
        "bigint": "int64",
        "float": "float",
        "double": "double",
        "binary": "binary",
        "string": "string",
        "varchar": "string",
    }
    return type_map[hive_type_as_str]
