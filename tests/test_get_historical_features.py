import os
import sys
import time
from datetime import datetime

from impala.dbapi import connect

from feast_hive import offline_store
from util import generate_entities


def get_env_var(name, default):
    if name in os.environ:
        return os.environ[name]
    else:
        sys.stderr.write(f"{name} not set; using {default}\n")
        return default


_HIVE_HOST = get_env_var("FEAST_HIVE_TEST_HOST", "localhost")
_HIVE_PORT = int(get_env_var("FEAST_HIVE_TEST_PORT", 10000))
_HIVE_AUTH_MECHANISM = get_env_var("FEAST_HIVE_TEST_PORT", "PLAIN")


def test_create_entity_table() -> None:
    with connect(_HIVE_HOST, _HIVE_PORT, auth_mechanism=_HIVE_AUTH_MECHANISM) as conn:
        cursor = conn.cursor()
        start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
        (_, _, _, entity_df, _,) = generate_entities(start_date, False)

        _start_time = time.time()
        table_ref_1 = offline_store._create_entity_table(
            "feast_hive_test", cursor, entity_df
        )
        print(f"Uploaded entity_df to {table_ref_1}")
        print(f"Uploading entity_df cost: {time.time() - _start_time} seconds.")
        cursor.execute(f"SELECT * FROM {table_ref_1}")
        hive_df = offline_store._get_dataframe_from_hive_data(
            cursor.fetchall(), cursor.description
        )
        print("=== Entity DF Dtypes: ===")
        print(entity_df.dtypes)
        print("=== Hive DF Dtypes:===")
        print(hive_df.dtypes)
        assert len(entity_df) == len(hive_df)

        _start_time = time.time()
        table_ref_2 = offline_store._create_entity_table(
            "feast_hive_test", cursor, f"SELECT * FROM {table_ref_1}"
        )
        print(f"Created entity_df by query to {table_ref_2}")
        print(f"Creating entity_df by query cost: {time.time() - _start_time} seconds.")
        cursor.execute(f"SELECT * FROM {table_ref_2}")
        hive_df2 = offline_store._get_dataframe_from_hive_data(
            cursor.fetchall(), cursor.description
        )
        print("=== Hive DF 2 Dtypes:===")
        print(hive_df2.dtypes)
        assert len(entity_df) == len(hive_df2)
