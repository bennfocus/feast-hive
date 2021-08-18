import os
import sys
import time
from datetime import datetime
from feast.infra.offline_stores import offline_utils

from impala.dbapi import connect

from feast_hive import offline_store
from tests.helper import generate_entities, with_cursor


@with_cursor
def test_upload_entity_df_and_get_entity_schema(cursor):
    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (_, _, _, entity_df, _,) = generate_entities(start_date, False)

    # Test upload entity_df to Hive
    table_name_1 = offline_utils.get_temp_entity_table_name()
    schema_1 = offline_store._upload_entity_df_and_get_entity_schema(
        table_name_1, entity_df, cursor
    )
    cursor.execute_async(f"SELECT * FROM {table_name_1}")
    new_entity_df_1 = offline_store.HiveRetrievalJob(cursor).to_df()
    assert len(entity_df) == len(new_entity_df_1)

    # Test upload by Query
    table_name_2 = offline_utils.get_temp_entity_table_name()
    schema_2 = offline_store._upload_entity_df_and_get_entity_schema(
        table_name_2, f"SELECT * FROM {table_name_1}", cursor
    )
    cursor.execute_async(f"SELECT * FROM {table_name_2}")
    new_entity_df_2 = offline_store.HiveRetrievalJob(cursor).to_df()
    assert new_entity_df_2.equals(new_entity_df_1)
