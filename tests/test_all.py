import contextlib
import os
import random
import time
import uuid
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Union, Iterator, Tuple, Any, Dict

import numpy as np
import pandas as pd
import pytest
from assertpy import assertpy
from feast import (
    ValueType,
    Entity,
    FeatureStore,
    RepoConfig,
    errors,
    FeatureView,
)
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from pandas._testing import assert_frame_equal

from feast_hive import (
    hive as feast_hive_module,
    HiveOfflineStoreConfig,
    HiveSource,
)
from feast_hive.hive import HiveConnection
from tests import feast_tests_funcs


@contextlib.contextmanager
def fake_upload_df_to_hive() -> Iterator[None]:
    yield


@contextlib.contextmanager
def temporarily_upload_df_to_hive(
    conn: HiveConnection, table_name: str, entity_df: Union[pd.DataFrame, str]
) -> Iterator[None]:
    try:
        feast_hive_module._upload_entity_df_by_insert(conn, table_name, entity_df)
        yield
    except Exception as ex:
        raise
    finally:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


def get_options_from_pytestconfig(pytestconfig,) -> Dict[str, Any]:
    return {
        "host": pytestconfig.getoption("host"),
        "port": int(pytestconfig.getoption("port")),
        "database": pytestconfig.getoption("database"),
    }


@pytest.fixture()
def hive_conn_info(pytestconfig):
    options = get_options_from_pytestconfig(pytestconfig)
    offline_store = HiveOfflineStoreConfig(**options)
    conn = feast_hive_module.HiveConnection(offline_store)

    yield offline_store, conn, options

    conn.close()


@contextlib.contextmanager
def prep_hive_fs_and_fv(
    offline_store, conn, source_type: str,
) -> Iterator[Tuple[FeatureStore, FeatureView]]:
    df = feast_tests_funcs.create_dataset()
    table_name = f"test_ingestion_{source_type}_correctness_{int(time.time_ns())}_{random.randint(1000, 9999)}"

    with temporarily_upload_df_to_hive(
        conn, table_name, df
    ), TemporaryDirectory() as repo_dir_name, TemporaryDirectory() as data_dir_name:

        hive_source = HiveSource(
            table=table_name if source_type == "table" else None,
            query=f"SELECT * FROM {table_name}" if source_type == "query" else None,
            event_timestamp_column="ts",
            created_timestamp_column="created_ts",
            date_partition_column="",
            field_mapping={"ts_1": "ts", "id": "driver_id"},
        )

        fv = feast_tests_funcs.correctness_feature_view(hive_source)
        e = Entity(
            name="driver",
            description="id for driver",
            join_key="driver_id",
            value_type=ValueType.INT32,
        )
        config = RepoConfig(
            registry=str(Path(repo_dir_name) / "registry.db"),
            project=f"test_bq_correctness_{str(uuid.uuid4()).replace('-', '')}",
            provider="local",
            online_store=SqliteOnlineStoreConfig(
                path=str(Path(data_dir_name) / "online_store.db")
            ),
            offline_store=offline_store,
        )
        fs = FeatureStore(config=config)
        fs.apply([fv, e])

        yield fs, fv

        fs.teardown()


def test_empty_result(hive_conn_info):
    offline_store, conn, _ = hive_conn_info

    empty_df = pd.DataFrame(columns=["a", "b", "c"], dtype=np.int32)
    table_name = f"test_empty_result_{int(time.time_ns())}_{random.randint(1000, 9999)}"

    with temporarily_upload_df_to_hive(conn, table_name, empty_df):
        sql_df_job = feast_hive_module.HiveRetrievalJob(
            conn, f"SELECT * FROM {table_name}"
        )
        sql_df = sql_df_job.to_df()
        assert sorted(sql_df.columns) == sorted(empty_df.columns)
        assert sql_df.empty


def test_hive_source(hive_conn_info):
    offline_store, conn, _ = hive_conn_info

    df = pd.DataFrame(
        {
            "a": [1.0, np.nan, 0.11122123123, 0.331412414132123123131231],
            "b": np.array([3] * 4, dtype="int32"),
            "c": ["foo", "oof", "ofo", None],
            "d": pd.date_range("2021-08-27", periods=4),
        }
    )
    table_name = f"test_hive_source_{int(time.time_ns())}_{random.randint(1000, 9999)}"
    expected_schema = [
        ("a", "DOUBLE"),
        ("b", "INT"),
        ("c", "STRING"),
        ("d", "TIMESTAMP"),
    ]

    with temporarily_upload_df_to_hive(
        conn, table_name, df
    ), TemporaryDirectory() as temp_dir:
        config = RepoConfig(
            registry=os.path.join(temp_dir, "registry.db"),
            project="default",
            provider="local",
            online_store=SqliteOnlineStoreConfig(
                path=os.path.join(temp_dir, "online_store.db"),
            ),
            offline_store=offline_store,
        )

        non_existed_table = f"{table_name}_non_existed"

        # Test table doesn't exist
        hive_source_table = HiveSource(table=non_existed_table)
        assertpy.assert_that(hive_source_table.validate).raises(
            errors.DataSourceNotFoundException
        ).when_called_with(config)

        hive_source_table = HiveSource(query=f"SELECT * FROM {non_existed_table}")
        assertpy.assert_that(hive_source_table.validate).raises(
            errors.DataSourceNotFoundException
        ).when_called_with(config)

        # Test table
        hive_source_table = HiveSource(table=table_name)
        schema1 = hive_source_table.get_table_column_names_and_types(config)
        assert expected_schema == schema1

        # Test query
        hive_source_table = HiveSource(query=f"SELECT * FROM {table_name} LIMIT 100")
        schema2 = hive_source_table.get_table_column_names_and_types(config)
        assert expected_schema == schema2


def test_upload_entity_df(hive_conn_info):
    offline_store, conn, _ = hive_conn_info

    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (_, _, _, orders_df, _,) = feast_tests_funcs.generate_entities(start_date, True)
    orders_table = f"test_upload_entity_df_orders_{int(time.time_ns())}_{random.randint(1000, 9999)}"
    with temporarily_upload_df_to_hive(conn, orders_table, orders_df):
        orders_df_from_sql = feast_hive_module.HiveRetrievalJob(
            conn, f"SELECT * FROM {orders_table}"
        ).to_df()

        assert sorted(orders_df.columns) == sorted(orders_df_from_sql.columns)
        assert_frame_equal(
            orders_df.sort_values(
                by=["e_ts", "order_id", "driver_id", "customer_id"]
            ).reset_index(drop=True),
            orders_df_from_sql[orders_df.columns]
            .sort_values(by=["e_ts", "order_id", "driver_id", "customer_id"])
            .reset_index(drop=True),
            check_dtype=False,
        )


def test_upload_abnormal_df(hive_conn_info):
    offline_store, conn, _ = hive_conn_info

    df1 = pd.DataFrame(
        {
            "a": [1.0, np.nan, 0.11122123123, 0.331412414132123123131231],
            # "b": [
            #     pd.Timestamp("20130102T12"),
            #     pd.Timestamp(1513393355.5, unit="s"),
            #     pd.Timestamp(1513393355, unit="s", tz="US/Pacific"),
            #     pd.Timestamp(year=2017, month=1, day=1, hour=12),
            # ],
            "c": pd.Series(1, index=list(range(4)), dtype="float32"),
            "d": np.array([3] * 4, dtype="int32"),
            "e": pd.Categorical(["test", "train", "test", "train"]),
            "f": ["foo", "oof", "ofo", None],
        }
    )
    df1_table = f"test_upload_abnormal_df_df1_{int(time.time_ns())}_{random.randint(1000, 9999)}"
    with temporarily_upload_df_to_hive(conn, df1_table, df1):
        df1_from_sql = feast_hive_module.HiveRetrievalJob(
            conn, f"SELECT * FROM {df1_table}"
        ).to_df()

        assert sorted(df1.columns) == sorted(df1_from_sql.columns)
        assert_frame_equal(
            df1.sort_values(by=["a"]).reset_index(drop=True),
            df1_from_sql[df1.columns].sort_values(by=["a"]).reset_index(drop=True),
            check_dtype=False,
            check_categorical=False,
        )


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
    provider_type, infer_event_timestamp_col, capsys, full_feature_names, hive_conn_info
):
    offline_store, conn, _ = hive_conn_info
    start_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    (
        customer_entities,
        driver_entities,
        end_date,
        orders_df,
        start_date,
    ) = feast_tests_funcs.generate_entities(start_date, infer_event_timestamp_col)

    hive_table_prefix = (
        f"test_hist_retrieval_{int(time.time_ns())}_{random.randint(1000, 9999)}"
    )

    # Stage orders_df to Hive
    orders_table_name = f"{hive_table_prefix}_orders"
    entity_df_query = f"SELECT * FROM {orders_table_name}"
    orders_context = temporarily_upload_df_to_hive(conn, orders_table_name, orders_df)

    # Stage driver_df to Hive
    driver_df = feast_tests_funcs.create_driver_hourly_stats_df(
        driver_entities, start_date, end_date
    )
    driver_table_name = f"{hive_table_prefix}_driver_hourly"
    driver_context = temporarily_upload_df_to_hive(conn, driver_table_name, driver_df)

    # Stage customer_df to Redshift
    customer_df = feast_tests_funcs.create_customer_daily_profile_df(
        customer_entities, start_date, end_date
    )
    customer_table_name = f"{hive_table_prefix}_customer_profile"
    customer_context = temporarily_upload_df_to_hive(
        conn, customer_table_name, customer_df
    )

    with orders_context, driver_context, customer_context, TemporaryDirectory() as temp_dir:
        driver_source = HiveSource(
            table=driver_table_name,
            event_timestamp_column="event_timestamp",
            created_timestamp_column="created",
        )
        driver_fv = feast_tests_funcs.create_driver_hourly_stats_feature_view(
            driver_source
        )

        customer_source = HiveSource(
            table=customer_table_name,
            event_timestamp_column="event_timestamp",
            created_timestamp_column="created",
        )
        customer_fv = feast_tests_funcs.create_customer_daily_profile_feature_view(
            customer_source
        )

        driver = Entity(name="driver", join_key="driver_id", value_type=ValueType.INT64)
        customer = Entity(name="customer_id", value_type=ValueType.INT64)

        if provider_type == "local":
            store = FeatureStore(
                config=RepoConfig(
                    registry=os.path.join(temp_dir, "registry.db"),
                    project="default",
                    provider="local",
                    online_store=SqliteOnlineStoreConfig(
                        path=os.path.join(temp_dir, "online_store.db"),
                    ),
                    offline_store=offline_store,
                )
            )
        else:
            raise Exception("Invalid provider used as part of test configuration")

        store.apply([driver, customer, driver_fv, customer_fv])

        try:
            event_timestamp = (
                feast_tests_funcs.DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
                if feast_tests_funcs.DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
                in orders_df.columns
                else "e_ts"
            )
            expected_df = feast_tests_funcs.get_expected_training_df(
                customer_df,
                customer_fv,
                driver_df,
                driver_fv,
                orders_df,
                event_timestamp,
                full_feature_names,
            )

            job_from_sql = store.get_historical_features(
                entity_df=entity_df_query,
                features=[
                    "driver_stats:conv_rate",
                    "driver_stats:avg_daily_trips",
                    "customer_profile:current_balance",
                    "customer_profile:avg_passenger_count",
                    "customer_profile:lifetime_trip_count",
                ],
                full_feature_names=full_feature_names,
            )

            start_time = datetime.utcnow()
            actual_df_from_sql_entities = job_from_sql.to_df()
            end_time = datetime.utcnow()
            with capsys.disabled():
                print(
                    str(
                        f"\nTime to execute job_from_sql.to_df() = '{(end_time - start_time)}'"
                    )
                )

            assert sorted(expected_df.columns) == sorted(
                actual_df_from_sql_entities.columns
            )
            assert_frame_equal(
                expected_df.sort_values(
                    by=[event_timestamp, "order_id", "driver_id", "customer_id"]
                ).reset_index(drop=True),
                actual_df_from_sql_entities[expected_df.columns]
                .sort_values(
                    by=[event_timestamp, "order_id", "driver_id", "customer_id"]
                )
                .reset_index(drop=True),
                check_dtype=False,
            )

            table_from_sql_entities = job_from_sql.to_arrow()
            assert_frame_equal(
                actual_df_from_sql_entities.sort_values(
                    by=[event_timestamp, "order_id", "driver_id", "customer_id"]
                ).reset_index(drop=True),
                table_from_sql_entities.to_pandas()
                .sort_values(
                    by=[event_timestamp, "order_id", "driver_id", "customer_id"]
                )
                .reset_index(drop=True),
            )

            timestamp_column = (
                "e_ts"
                if infer_event_timestamp_col
                else feast_tests_funcs.DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
            )

            entity_df_query_with_invalid_join_key = (
                f"select order_id, driver_id, customer_id as customer, "
                f"order_is_success, {timestamp_column} FROM {orders_table_name}"
            )
            # Rename the join key; this should now raise an error.
            assertpy.assert_that(
                store.get_historical_features(
                    entity_df=entity_df_query_with_invalid_join_key,
                    features=[
                        "driver_stats:conv_rate",
                        "driver_stats:avg_daily_trips",
                        "customer_profile:current_balance",
                        "customer_profile:avg_passenger_count",
                        "customer_profile:lifetime_trip_count",
                    ],
                ).to_df
            ).raises(errors.FeastEntityDFMissingColumnsError).when_called_with()

            job_from_df = store.get_historical_features(
                entity_df=orders_df,
                features=[
                    "driver_stats:conv_rate",
                    "driver_stats:avg_daily_trips",
                    "customer_profile:current_balance",
                    "customer_profile:avg_passenger_count",
                    "customer_profile:lifetime_trip_count",
                ],
                full_feature_names=full_feature_names,
            )

            # Rename the join key; this should now raise an error.
            orders_df_with_invalid_join_key = orders_df.rename(
                {"customer_id": "customer"}, axis="columns"
            )
            assertpy.assert_that(
                store.get_historical_features(
                    entity_df=orders_df_with_invalid_join_key,
                    features=[
                        "driver_stats:conv_rate",
                        "driver_stats:avg_daily_trips",
                        "customer_profile:current_balance",
                        "customer_profile:avg_passenger_count",
                        "customer_profile:lifetime_trip_count",
                    ],
                ).to_df
            ).raises(errors.FeastEntityDFMissingColumnsError).when_called_with()

            start_time = datetime.utcnow()
            actual_df_from_df_entities = job_from_df.to_df()
            end_time = datetime.utcnow()
            with capsys.disabled():
                print(
                    str(
                        f"Time to execute job_from_df.to_df() = '{(end_time - start_time)}'\n"
                    )
                )

            assert sorted(expected_df.columns) == sorted(
                actual_df_from_df_entities.columns
            )
            assert_frame_equal(
                expected_df.sort_values(
                    by=[event_timestamp, "order_id", "driver_id", "customer_id"]
                ).reset_index(drop=True),
                actual_df_from_df_entities[expected_df.columns]
                .sort_values(
                    by=[event_timestamp, "order_id", "driver_id", "customer_id"]
                )
                .reset_index(drop=True),
                check_dtype=False,
            )

            table_from_df_entities = job_from_df.to_arrow()
            assert_frame_equal(
                actual_df_from_df_entities.sort_values(
                    by=[event_timestamp, "order_id", "driver_id", "customer_id"]
                ).reset_index(drop=True),
                table_from_df_entities.to_pandas()
                .sort_values(
                    by=[event_timestamp, "order_id", "driver_id", "customer_id"]
                )
                .reset_index(drop=True),
            )
        except Exception as ex:
            raise
        finally:
            store.teardown()


@pytest.mark.parametrize(
    "source_type", ["query", "table"],
)
@pytest.mark.parametrize("full_feature_names", [True, False])
def test_hive_offline_online_store_consistency(
    hive_conn_info, source_type: str, full_feature_names: bool
):
    offline_store, conn, _ = hive_conn_info
    with prep_hive_fs_and_fv(offline_store, conn, source_type) as (fs, fv):
        feast_tests_funcs.run_offline_online_store_consistency_test(
            fs, fv, full_feature_names
        )


def test_hive_config(pytestconfig):
    options = get_options_from_pytestconfig(pytestconfig)
    options["hive_conf"] = {  # give it some special values
        "hive.join.cache.size": 14797,
        "hive.exec.max.dynamic.partitions": 779,
    }
    offline_store = HiveOfflineStoreConfig(**options)
    with feast_hive_module.HiveConnection(offline_store) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SET hive.join.cache.size")
            expected = ("hive.join.cache.size=14797",)
            assert cursor.fetchone() == expected

            cursor.execute("SET hive.exec.max.dynamic.partitions")
            expected = ("hive.exec.max.dynamic.partitions=779",)
            assert cursor.fetchone() == expected
