import contextlib
from datetime import datetime
from typing import Callable, ContextManager, Dict, Iterator, List, Optional, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from pydantic import StrictBool, StrictInt, StrictStr
from pytz import utc
from six import reraise

from feast import FeatureView
from feast.data_source import DataSource
from feast.errors import InvalidEntityType
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast_hive.hive_source import HiveSource
from feast_hive.hive_type_map import hive_to_pa_value_type, pa_to_hive_value_type

try:
    from impala.dbapi import connect as impala_connect
    from impala.hiveserver2 import CBatch as ImpalaCBatch
    from impala.hiveserver2 import Column as ImpalaColumn
    from impala.interface import Connection as ImpalaConnection
    from impala.interface import Cursor as ImpalaCursor
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("hive", str(e))


class HiveOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for Hive """

    type: StrictStr = "hive"
    """ Offline store type selector """

    host: StrictStr
    """ Host name of the HiveServer2 """

    port: StrictInt = 10000
    """ Port number of HiveServer2 (default is 10000) """

    database: StrictStr = "default"
    """ Default database when obtaining new cursors """

    timeout: Optional[StrictInt] = None
    """ Connection timeout in seconds when communicating with HiveServer2 """

    hive_conf: Optional[Dict[str, str]] = None
    """ Configuration overlay for the HiveServer2 session """

    entity_uploading_chunk_size: StrictInt = 10000
    """ The chunk size of multiple insert when uploading entity_df to Hive,
        tuning this may affect the performance of get_historical_features """

    use_ssl: StrictBool = False
    """ Use SSL when connecting to HiveServer2 """

    ca_cert: Optional[StrictStr] = None
    """ Local path to the the third-party CA certificate. If SSL is enabled but
        the certificate is not specified, the server certificate will not be
        validated. """

    auth_mechanism: StrictStr = "PLAIN"
    """ {'NOSASL', 'PLAIN' <- default, 'GSSAPI', 'LDAP'}.
        Use PLAIN for non-secured Hive clusters.  Use NOSASL for 
        non-secured Impala connections.  Use LDAP for LDAP authenticated
        connections.  Use GSSAPI for Kerberos-secured clusters. """

    user: Optional[StrictStr] = None
    """ LDAP user to authenticate """

    password: Optional[StrictStr] = None
    """ LDAP password to authenticate """

    kerberos_service_name: StrictStr = "impala"
    """ Specify particular impalad service principal. """


class HiveConnection:
    def __init__(self, store_config: HiveOfflineStoreConfig):
        assert isinstance(store_config, HiveOfflineStoreConfig)
        self._store_config = store_config
        self._real_conn = impala_connect(
            **store_config.dict(
                exclude={"type", "entity_uploading_chunk_size", "hive_conf"}
            )
        )

    @property
    def real_conn(self) -> ImpalaConnection:
        return self._real_conn

    def close(self) -> None:
        self.real_conn.close()

    def cursor(self) -> ImpalaCursor:
        if self._store_config.hive_conf:
            return self.real_conn.cursor(configuration=self._store_config.hive_conf)
        else:
            return self.real_conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_type is not None:
            reraise(exc_type, exc_val, exc_tb)


class HiveOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, HiveOfflineStoreConfig)
        assert isinstance(data_source, HiveSource)

        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamps = [event_timestamp_column]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        field_string = ", ".join(join_key_columns + feature_name_columns + timestamps)

        start_date = _format_datetime(start_date)
        end_date = _format_datetime(end_date)

        query = f"""
                SELECT {field_string}
                FROM (
                    SELECT {field_string},
                    ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS feast_row_
                    FROM {from_expression} t1
                    WHERE {event_timestamp_column} BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
                ) t2
                WHERE feast_row_ = 1
                """

        conn = HiveConnection(config.offline_store)
        return HiveRetrievalJob(conn, query)

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, HiveOfflineStoreConfig)
        conn = HiveConnection(config.offline_store)

        @contextlib.contextmanager
        def query_generator() -> Iterator[List[str]]:
            table_name = offline_utils.get_temp_entity_table_name()

            try:
                entity_schema = _upload_entity_df_and_get_entity_schema(
                    config, conn, table_name, entity_df
                )

                entity_df_event_timestamp_col = offline_utils.infer_event_timestamp_from_entity_df(
                    entity_schema
                )

                expected_join_keys = offline_utils.get_expected_join_keys(
                    project, feature_views, registry
                )

                offline_utils.assert_expected_columns_in_entity_df(
                    entity_schema, expected_join_keys, entity_df_event_timestamp_col
                )

                query_context = offline_utils.get_feature_view_query_context(
                    feature_refs, feature_views, registry, project,
                )

                rendered_query = offline_utils.build_point_in_time_query(
                    query_context,
                    left_table_query_string=table_name,
                    entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                    query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                    full_feature_names=full_feature_names,
                )

                # In order to use `REGEX Column Specification`, need set `hive.support.quoted.identifiers` to None.
                # Can study more here: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select
                queries = [
                    "SET hive.support.quoted.identifiers=None",
                    "SET hive.exec.temporary.table.storage=memory",
                ] + rendered_query.split(";")

                yield queries

            finally:
                with conn.cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        return HiveRetrievalJob(conn, query_generator)


class HiveRetrievalJob(RetrievalJob):
    def __init__(
        self,
        conn: HiveConnection,
        queries: Union[str, List[str], Callable[[], ContextManager[List[str]]]],
    ):
        assert (
            isinstance(queries, str) or isinstance(queries, list) or callable(queries)
        )

        if callable(queries):
            self._queries_generator = queries
        else:
            if isinstance(queries, str):
                queries = [queries]

            @contextlib.contextmanager
            def query_generator() -> Iterator[List[str]]:
                yield queries

            self._queries_generator = query_generator

        self._conn = conn

    def to_df(self) -> pd.DataFrame:
        return self.to_arrow().to_pandas()

    def to_arrow(self) -> pa.Table:
        with self._queries_generator() as queries:
            with self._conn.cursor() as cursor:
                for query in queries:
                    cursor.execute(query)
                batches = cursor.fetchcolumnar()
                schema = pa.schema(
                    [
                        (field[0], hive_to_pa_value_type(field[1]))
                        for field in cursor.description
                    ]
                )
                pa_batches = [
                    HiveRetrievalJob._convert_hive_batch_to_arrow_batch(b, schema)
                    for b in batches
                ]
                return pa.Table.from_batches(pa_batches, schema)

    @staticmethod
    def _convert_hive_batch_to_arrow_batch(
        hive_batch: ImpalaCBatch, schema: pa.Schema
    ) -> pa.RecordBatch:
        return pa.record_batch(
            [
                HiveRetrievalJob._get_values_from_column(column)
                for column in hive_batch.columns
            ],
            schema,
        )

    @staticmethod
    def _get_values_from_column(column: ImpalaColumn) -> List:
        values = column.values
        for i in range(len(values)):
            if column.nulls[i]:
                values[i] = None
        return values


def _format_datetime(t: datetime):
    # Since Hive does not support timezone, need to transform to utc.
    if t.tzinfo:
        t = t.astimezone(tz=utc)
    t = t.strftime("%Y-%m-%d %H:%M:%S.%f")
    return t


def _upload_entity_df_and_get_entity_schema(
    config: RepoConfig,
    conn: HiveConnection,
    table_name: str,
    entity_df: Union[pd.DataFrame, str],
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        _upload_entity_df_by_insert(
            conn,
            table_name,
            entity_df,
            config.offline_store.entity_uploading_chunk_size,
        )
        return dict(zip(entity_df.columns, entity_df.dtypes))
    elif isinstance(entity_df, str):
        with conn.cursor() as cursor:
            cursor.execute(
                f"CREATE TABLE {table_name} STORED AS PARQUET AS {entity_df}"
            )
        limited_entity_df = HiveRetrievalJob(
            conn, f"SELECT * FROM {table_name} LIMIT 1"
        ).to_df()
        return dict(zip(limited_entity_df.columns, limited_entity_df.dtypes))
    else:
        raise InvalidEntityType(type(entity_df))


def _upload_entity_df_by_insert(
    conn: HiveConnection,
    table_name: str,
    entity_df: Union[pd.DataFrame, str],
    chunk_size: int = None,
) -> None:
    """Uploads a Pandas DataFrame to Hive by data insert"""
    entity_df.reset_index(drop=True, inplace=True)

    pa_table = pa.Table.from_pandas(entity_df)
    hive_schema = []
    for field in pa_table.schema:
        hive_type = pa_to_hive_value_type(str(field.type))
        if not hive_type:
            raise ValueError(f'Not supported type "{field.type}" in entity_df.')
        hive_schema.append((field.name, hive_type))

    with conn.cursor() as cursor:
        # Create Hive temporary table according to entity_df schema
        create_entity_table_sql = f"""
            CREATE TABLE {table_name} (
              {', '.join([f'{col_name} {col_type}' for col_name, col_type in hive_schema])}
            )
            STORED AS PARQUET
            """
        cursor.execute(create_entity_table_sql)

        def preprocess_value(raw_value, col_type):
            if raw_value is None:
                return "null"

            col_type = col_type.lower()

            if col_type == "timestamp":
                if isinstance(raw_value, datetime):
                    raw_value = _format_datetime(raw_value)
                    return f'"{raw_value}"'
                else:
                    return f'"{raw_value}"'

            if col_type in ["string", "date"]:
                return f'"{raw_value}"'
            else:
                return str(raw_value)

        # Upload entity_df to the Hive table by multiple rows insert method
        entity_count = len(pa_table)
        chunk_size = entity_count if chunk_size is None else chunk_size
        pa_batches = pa_table.to_batches(chunk_size)
        if len(pa_batches) > 1:
            # fix this hive bug: https://issues.apache.org/jira/browse/HIVE-19316
            cursor.execute(f"truncate table {table_name}")
        for batch in pa_batches:
            chunk_data = []
            for i in range(len(batch)):
                chunk_data.append(
                    [
                        preprocess_value(batch.columns[j][i].as_py(), hive_schema[j][1])
                        for j in range(len(hive_schema))
                    ]
                )

            entity_chunk_insert_sql = f"""
                INSERT INTO TABLE {table_name}
                VALUES ({'), ('.join([', '.join(chunk_row) for chunk_row in chunk_data])})
            """
            cursor.execute(entity_chunk_insert_sql)


# This query is based on sdk/python/feast/infra/offline_stores/bigquery.py:MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN
# There are many changes compare to the BigQuery one:
#
# 1. Use "t - interval 'x' second" instead of "Timestamp_sub(...)".
# 2. Replace `SELECT * EXCEPT (...)` with `REGEX Column Specification`, because `EXCEPT` is not supported by Hive.
#    Can study more here: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Select.
# 3. Change `USING` to `ON`, since Hive doesn't support `USING`.
# 4. Change ANY_VALUE() to MAX(), since Hive doesn't support `ANY_VALUE`.
# 5. Change Subquery `SELECT MAX(entity_timestamp) FROM entity_dataframe` and `SELECT MIN(entity_timestamp) FROM
#    entity_dataframe` to `INNER JOIN`. Since Hive does not support more than a single Subquery, and doesn't supports
#    Subquery on non-top level.
#
# 6. Mostly big change here is that I tested the original Query on my own Hive set ups (3.0.0 and 3.1.2, based on Spark
#    and Yarn), the CTE didn't work, `*__cleaned` had no result, but after I changed the CTE to Temporary Table it
#    worked. Not sure if it was a common case, but here I changed it to the workable Temporary Table.
# 7. Move `(MIN(entity_timestamp) - interval 'ttl' second)` to SELECT clause, since Temporary Table
#    does support it in Where clause.
#
# In case there was any improvement on BigQuery, probably need to apply here too.

MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
CREATE TEMPORARY TABLE entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            ,CONCAT(
                {% for entity in featureview.entities %}
                    CAST({{entity}} AS STRING),
                {% endfor %}
                CAST({{entity_df_event_timestamp_col}} AS STRING)
            ) AS {{featureview.name}}__entity_row_unique_id
        {% endfor %}
    FROM {{ left_table_query_string }}
);


-- Start create temporary table *__base
{% for featureview in featureviews %}

CREATE TEMPORARY TABLE {{ featureview.name }}__base AS
WITH {{ featureview.name }}__entity_dataframe AS (
    SELECT
        {{ featureview.entities | join(', ')}},
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY {{ featureview.entities | join(', ')}}, entity_timestamp, {{featureview.name}}__entity_row_unique_id
),

/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.

 1. We first join the current feature_view to the entity dataframe that has been passed.
 This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `event_timestamp_column`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `event_timestamp_column`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously

 The output of this CTE will contain all the necessary information and already filtered out most
 of the data that is not relevant.
*/

{{ featureview.name }}__subquery AS (
    SELECT
        {{ featureview.event_timestamp_column }} as event_timestamp,
        {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}},
        {% for feature in featureview.features %}
            {{ feature }} as {% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }} AS subquery
    INNER JOIN (
        SELECT MAX(entity_timestamp) as max_entity_timestamp_
               {% if featureview.ttl == 0 %}{% else %}
               ,(MIN(entity_timestamp) - interval '{{ featureview.ttl }}' second) as min_entity_timestamp_
               {% endif %}
        FROM entity_dataframe
    ) AS temp
    ON (
        {{ featureview.event_timestamp_column }} <= max_entity_timestamp_
        {% if featureview.ttl == 0 %}{% else %}
        AND {{ featureview.event_timestamp_column }} >=  min_entity_timestamp_
        {% endif %}
    )
)
SELECT
    subquery.*,
    entity_dataframe.entity_timestamp,
    entity_dataframe.{{featureview.name}}__entity_row_unique_id
FROM {{ featureview.name }}__subquery AS subquery
INNER JOIN (
    SELECT *
    {% if featureview.ttl == 0 %}{% else %}
    , (entity_timestamp - interval '{{ featureview.ttl }}' second) as ttl_entity_timestamp
    {% endif %}
    FROM {{ featureview.name }}__entity_dataframe
) AS entity_dataframe
ON ( 
    subquery.event_timestamp <= entity_dataframe.entity_timestamp

    {% if featureview.ttl == 0 %}{% else %}
    AND subquery.event_timestamp >= entity_dataframe.ttl_entity_timestamp
    {% endif %}

    {% for entity in featureview.entities %}
    AND subquery.{{ entity }} = entity_dataframe.{{ entity }}
    {% endfor %}
);

{% endfor %}
-- End create temporary table *__base


{% for featureview in featureviews %}

{% if loop.first %}WITH{% endif %}

/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
{{ featureview.name }}__dedup AS (
    SELECT
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp,
        MAX(created_timestamp) as created_timestamp
    FROM {{ featureview.name }}__base
    GROUP BY {{featureview.name}}__entity_row_unique_id, event_timestamp
),
{% endif %}

/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
{{ featureview.name }}__latest AS (
    SELECT
        base.{{featureview.name}}__entity_row_unique_id,
        MAX(base.event_timestamp) AS event_timestamp
        {% if featureview.created_timestamp_column %}
            ,MAX(base.created_timestamp) AS created_timestamp
        {% endif %}

    FROM {{ featureview.name }}__base AS base
    {% if featureview.created_timestamp_column %}
        INNER JOIN {{ featureview.name }}__dedup AS dedup
        ON (
            dedup.{{featureview.name}}__entity_row_unique_id=base.{{featureview.name}}__entity_row_unique_id
            AND dedup.event_timestamp=base.event_timestamp
            AND dedup.created_timestamp=base.created_timestamp
        )
    {% endif %}

    GROUP BY base.{{featureview.name}}__entity_row_unique_id
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
{{ featureview.name }}__cleaned AS (
    SELECT base.*
    FROM {{ featureview.name }}__base AS base
    INNER JOIN {{ featureview.name }}__latest AS latest
    ON (
        base.{{featureview.name}}__entity_row_unique_id=latest.{{featureview.name}}__entity_row_unique_id
        AND base.event_timestamp=latest.event_timestamp
        {% if featureview.created_timestamp_column %}
            AND base.created_timestamp=latest.created_timestamp
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}


{% endfor %}

/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT `(entity_timestamp|{% for featureview in featureviews %}{{featureview.name}}__entity_row_unique_id{% if loop.last %}{% else %}|{% endif %}{% endfor %})?+.+`
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) AS {{ featureview.name }}__joined 
ON (
    {{ featureview.name }}__joined.{{featureview.name}}__entity_row_unique_id=entity_dataframe.{{featureview.name}}__entity_row_unique_id
)
{% endfor %}
"""
