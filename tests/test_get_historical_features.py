import os
import sys
import time
from datetime import datetime

from impala.dbapi import connect
import pandas as pd

from feast_hive import offline_store
from .util import generate_entities


def get_env_var(name, default):
    if name in os.environ:
        return os.environ[name]
    else:
        sys.stderr.write(f"{name} not set; using {default}\n")
        return default


_HIVE_HOST = get_env_var("FEAST_HIVE_TEST_HOST", "localhost")
_HIVE_PORT = int(get_env_var("FEAST_HIVE_TEST_PORT", 10000))
_HIVE_AUTH_MECHANISM = get_env_var("FEAST_HIVE_TEST_PORT", "PLAIN")


def test_upload_entity_df_to_hive() -> None:
    start_time = time.time()
    with connect(_HIVE_HOST, _HIVE_PORT, auth_mechanism=_HIVE_AUTH_MECHANISM) as conn:
        entity_table_ref = f"feast_hive_test_entity_df_{int(time.time())}"
        cursor = conn.cursor()

        start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
        # (_, _, _, entity_df, _,) = generate_entities(start_date, False)
        entity_df = pd.DataFrame.from_dict(
            {
                "driver_id": [1001, 1002, 1003, 1004],
                "event_timestamp": [
                    datetime(2021, 4, 12, 10, 59, 42),
                    datetime(2021, 4, 12, 8, 12, 10),
                    datetime(2021, 4, 12, 16, 40, 26),
                    datetime(2021, 4, 12, 15, 1, 12),
                ],
            }
        )

        offline_store._upload_entity_df_to_hive(entity_table_ref, cursor, entity_df)
        print(f"cost time: {time.time() - start_time}")

        cursor.execute(f"SELECT * FROM {entity_table_ref}")
        hive_df = offline_store._get_dataframe_from_hive_data(
            cursor.fetchall(), cursor.description
        )
        print("Entity DF Dtypes:")
        print(entity_df.dtypes)
        print("Hive DF Dtypes:")
        print(hive_df.dtypes)
        assert entity_df.equals(hive_df)
