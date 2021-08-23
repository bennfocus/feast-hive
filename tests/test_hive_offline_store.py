import os
import random
import sys
import time
from datetime import datetime

import pytest
from feast.infra.offline_stores import offline_utils

from impala.dbapi import connect

from feast_hive import offline_store, HiveOfflineStoreConfig
from tests.helper import generate_entities, get_hive_cursor, with_cursor


# @with_cursor
# def test_upload_entity_df(cursor):
#     start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
#     (_, _, _, entity_df, _,) = generate_entities(start_date, False)

#     # Test upload entity_df to Hive
#     table_name_1 = offline_utils.get_temp_entity_table_name()
#     schema_1 = offline_store._upload_entity_df_and_get_entity_schema(
#         table_name_1, entity_df, cursor
#     )
#     cursor.execute_async(f"SELECT * FROM {table_name_1}")
#     new_entity_df_1 = offline_store.HiveRetrievalJob(cursor).to_df()
#     assert len(entity_df) == len(new_entity_df_1)

#     # Test upload by Query
#     table_name_2 = offline_utils.get_temp_entity_table_name()
#     schema_2 = offline_store._upload_entity_df_and_get_entity_schema(
#         table_name_2, f"SELECT * FROM {table_name_1}", cursor
#     )
#     cursor.execute_async(f"SELECT * FROM {table_name_2}")
#     new_entity_df_2 = offline_store.HiveRetrievalJob(cursor).to_df()
#     assert new_entity_df_2.equals(new_entity_df_1)


@pytest.mark.parametrize(
    "provider_type", ["local"],
)
@pytest.mark.parametrize(
    "infer_event_timestamp_col", [False, True],
)
@pytest.mark.parametrize(
    "full_feature_names", [False, True],
)
def test_historical_features_from_hive_sources(
    provider_type, infer_event_timestamp_col, capsys, full_feature_names, pytestconfig
):
    hive_offline_store = HiveOfflineStoreConfig(
        host=pytestconfig.getoption("hive_host"),
        port=int(pytestconfig.getoption("hive_port")),
    )

    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (
        customer_entities,
        driver_entities,
        end_date,
        orders_df,
        start_date,
    ) = generate_entities(start_date, infer_event_timestamp_col)

    table_prefix = (
        f"test_hist_retrieval_{int(time.time_ns())}_{random.randint(1000, 9999)}"
    )

    # Stage orders_df to Hive
    table_name = f"{table_prefix}_orders"
    entity_df_query = f"SELECT * FROM {table_name}"
    schema_1 = offline_store._upload_entity_df(table_name, orders_df, cursor)
    orders_context = aws_utils.temporarily_upload_df_to_redshift(
        client,
        offline_store.cluster_id,
        offline_store.database,
        offline_store.user,
        s3,
        f"{offline_store.s3_staging_location}/copy/{table_name}.parquet",
        offline_store.iam_role,
        table_name,
        orders_df,
    )
