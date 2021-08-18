import os
import sys
import time
from datetime import datetime
from feast.infra.offline_stores import offline_utils

from impala.dbapi import connect

from feast_hive import offline_store
from tests.helper import generate_entities, with_cursor


@with_cursor
def test_upload_entity_df(cursor):
    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (_, _, _, entity_df, _,) = generate_entities(start_date, False)

    _start_time = time.time()
    table_name_1 = offline_utils.get_temp_entity_table_name()
    offline_store._upload_entity_df(table_name_1, entity_df, cursor)
    print(f"Uploaded entity_df to {table_name_1}")
    print(f"Uploading entity_df cost: {time.time() - _start_time} seconds.")
    cursor.execute(f"SELECT * FROM {table_name_1}")
    pa_table = offline_store._convert_hive_data_to_arrow(
        cursor.fetchall(), cursor.description
    )
    assert len(entity_df) == len(pa_table)

    # _start_time = time.time()
    # table_ref_2 = offline_store._create_entity_table(
    #     "feast_hive_test", cursor, f"SELECT * FROM {table_name_1}"
    # )
    # print(f"Created entity_df by query to {table_ref_2}")
    # print(f"Creating entity_df by query cost: {time.time() - _start_time} seconds.")
    # cursor.execute(f"SELECT * FROM {table_ref_2}")
    # hive_df2 = offline_store._convert_hive_data_to_arrow(
    #     cursor.fetchall(), cursor.description
    # )
    # print("=== Hive DF 2 Dtypes:===")
    # print(hive_df2.dtypes)
    # assert len(entity_df) == len(hive_df2)
