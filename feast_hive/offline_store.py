import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from pydantic import StrictBool, StrictInt, StrictStr
from pydantic.typing import Literal

from feast import FeatureView
from feast.data_source import DataSource
from feast.errors import InvalidEntityType
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.value_type import ValueType
from feast_hive.data_source import HiveSource
from feast_hive.type_map import hive_to_pa_value_type, pa_to_hive_value_type

try:
    from impala.dbapi import connect
    from impala.hiveserver2 import HiveServer2Cursor, OperationalError

except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("hive", str(e))


_Cursor = HiveServer2Cursor


class HiveOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for Hive """

    type: Literal["hive"] = "hive"
    """ Offline store type selector """

    host: StrictStr
    """ The hostname for HiveServer2 """

    port: StrictInt = 10000
    """ The port number for HiveServer2 (default is 10000) """

    database: Optional[StrictStr] = None
    """ The default database. If `None`, the result is implementation-dependent """

    timeout: Optional[StrictInt] = None
    """ Connection timeout in seconds. Default is no timeout """

    use_ssl: StrictBool = False
    """ Enable SSL """

    ca_cert: Optional[StrictStr] = None
    """ Local path to the the third-party CA certificate. If SSL is enabled but
        the certificate is not specified, the server certificate will not be
        validated. """

    auth_mechanism: StrictStr = "PLAIN"
    """ Specify the authentication mechanism. `'PLAIN'` for unsecured, `'GSSAPI'` for Kerberos and `'LDAP'` for Kerberos with
        LDAP. """

    user: Optional[StrictStr] = None
    """ LDAP user, if applicable. """

    password: Optional[StrictStr] = None
    """ LDAP password, if applicable. """

    use_http_transport: StrictBool = False
    """ Set it to True to use http transport of False to use binary transport. """

    http_path: StrictStr = ""
    """ Specify the path in the http URL. Used only when `use_http_transport` is True. """


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

        from_expression = data_source.table

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

        with connect(**config.offline_store.dict(exclude={"type"})) as conn:
            with conn.cursor() as cursor:
                query = f"""
                SELECT {field_string}
                FROM (
                    SELECT {field_string},
                    ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                    FROM {from_expression} t1
                    WHERE {event_timestamp_column} BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
                ) t2
                WHERE _feast_row = 1
                """

                # execute the job now asynchronously, then block in to_df or to_arrow
                cursor.execute_async(query)
                return HiveRetrievalJob(cursor)

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

        with connect(**config.offline_store.dict(exclude={"type"})) as conn:
            with conn.cursor() as cursor:

                table_name = offline_utils.get_temp_entity_table_name()

                entity_schema = _upload_entity_df_and_get_entity_schema(
                    table_name, entity_df, cursor
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

                query = offline_utils.build_point_in_time_query(
                    query_context,
                    left_table_query_string=table_name,
                    entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                    query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                    full_feature_names=full_feature_names,
                )

                # execute the job now asynchronously, then block in to_df or to_arrow
                cursor.execute_async(query)
                return HiveRetrievalJob(cursor)


class HiveRetrievalJob(RetrievalJob):
    def __init__(self, cursor: _Cursor):
        self._cursor = cursor

    def to_df(self) -> pd.DataFrame:
        return self.to_arrow().to_pandas()

    def to_arrow(self) -> pa.Table:
        self._wait_results()
        return _convert_hive_data_to_arrow(
            self._cursor.fetchall(), self._cursor.description
        )

    def _wait_results(self):
        loop_start = time.time()
        cur = self._cursor
        try:
            while True:
                state = cur.status()
                if cur._op_state_is_error(state):
                    raise OperationalError("Operation is in ERROR_STATE")
                if not cur._op_state_is_executing(state):
                    break
                time.sleep(self._get_sleep_interval(loop_start))
        except KeyboardInterrupt:
            print("Canceling query")
            cur.cancel_operation()
            raise

    @staticmethod
    def _get_sleep_interval(self, start_time) -> float:
        """Returns a step function of time to sleep in seconds before polling
        again. Maximum sleep is 1s, minimum is 0.1s"""
        elapsed = time.time() - start_time
        if elapsed < 0.05:
            return 0.01
        elif elapsed < 1.0:
            return 0.05
        elif elapsed < 10.0:
            return 0.1
        elif elapsed < 60.0:
            return 0.5
        return 1.0


def _upload_entity_df_and_get_entity_schema(
    table_name: str, entity_df: Union[pd.DataFrame, str], cursor: _Cursor,
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        _upload_entity_df(table_name, entity_df, cursor)
        return dict(zip(entity_df.columns, entity_df.dtypes))
    elif isinstance(entity_df, str):
        cursor.execute(f"CREATE TEMPORARY TABLE {table_name} AS ({entity_df})")
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
        limited_entity_df = _convert_hive_data_to_arrow(
            cursor.fetchall(), cursor._description
        )
        return dict(zip(limited_entity_df.columns, limited_entity_df.dtypes))
    else:
        raise InvalidEntityType(type(entity_df))


# Size of each chunk when upload entity_df to Hive
_ENTITY_UPLOADING_CHUNK_SIZE = 10000


def _upload_entity_df(
    table_name: str, entity_df: Union[pd.DataFrame, str], cursor: _Cursor
) -> None:
    """Uploads a Pandas DataFrame to Hive as a temporary table (only exists in current session).

    It uses multiple row insert method to upload the Dataframe to Hive, in order to reduce the complexity.
    In future if we got performance issue, can consider to transform to a parquet file and upload to HDFS first.

    """
    entity_df.reset_index(drop=True, inplace=True)

    pa_schema = pa.Schema.from_pandas(entity_df)
    schema = []
    for field in pa_schema:
        hive_type = pa_to_hive_value_type(str(field.type))
        if not hive_type:
            raise ValueError(f'Not supported type "{field.type}" in entity_df.')
        schema.append((field.name, hive_type))

    # Create Hive temporary table according to entity_df schema
    create_entity_table_sql = f"""
        CREATE TEMPORARY TABLE {table_name} (
          {', '.join([f'{col_name} {col_type}' for col_name, col_type in schema])}
        )
        """
    cursor.execute(create_entity_table_sql)

    def wrap_sql_value(value, col_type):
        if col_type.lower() in ["string", "timestamp", "date"]:
            return f'"{value}"'
        else:
            return value

    # Upload entity_df to the Hive table by multiple rows insert method
    entity_count = len(entity_df)
    chunk_size = (
        entity_count
        if _ENTITY_UPLOADING_CHUNK_SIZE <= 0
        else _ENTITY_UPLOADING_CHUNK_SIZE
    )
    chunks = (entity_count // chunk_size) + 1
    for i in range(chunks):
        start_i = i * chunk_size
        end_i = min((i + 1) * chunk_size, entity_count)
        if start_i >= end_i:
            break

        chunk_data = []
        for j in range(start_i, end_i):
            chunk_data.append(
                [
                    wrap_sql_value(str(entity_df.loc[j, col_name]), col_type)
                    for col_name, col_type in schema
                ]
            )

        insert_entity_chunk_sql = f"""
            INSERT INTO TABLE {table_name}
            VALUES ({'), ('.join([', '.join(entity_row) for entity_row in chunk_data])})
        """
        cursor.execute(insert_entity_chunk_sql)


def _convert_hive_data_to_arrow(
    data: Dict[str, Any], schema: List[Tuple[str, str]]
) -> pa.Table:
    pa_schema = pa.schema(
        [(field[0], hive_to_pa_value_type(field[1])) for field in schema]
    )
    return pa.table(data, schema=pa_schema)


# This query is based on sdk/python/feast/infra/offline_stores/bigquery.py:MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN
# There are couple of changes from BigQuery:
# 1. Use VARCHAR instead of STRING type
# 2. Use "t - x * interval '1' second" instead of "Timestamp_sub(...)"
# 3. Replace `SELECT * EXCEPT (...)` with `SELECT *`, because `EXCEPT` is not supported by Hive.
#    Instead, we drop the column later after creating the table out of the query.
# We need to keep this query in sync with BigQuery.

MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
WITH entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            ,CONCAT(
                {% for entity in featureview.entities %}
                    CAST({{entity}} AS VARCHAR),
                {% endfor %}
                CAST({{entity_df_event_timestamp_col}} AS VARCHAR)
            ) AS {{featureview.name}}__entity_row_unique_id
        {% endfor %}
    FROM {{ left_table_query_string }}
),

{% for featureview in featureviews %}

{{ featureview.name }}__entity_dataframe AS (
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
    FROM {{ featureview.table_subquery }}
    WHERE {{ featureview.event_timestamp_column }} <= (SELECT MAX(entity_timestamp) FROM entity_dataframe)
    {% if featureview.ttl == 0 %}{% else %}
    AND {{ featureview.event_timestamp_column }} >= (SELECT MIN(entity_timestamp) FROM entity_dataframe) - {{ featureview.ttl }} * interval '1' second
    {% endif %}
),

{{ featureview.name }}__base AS (
    SELECT
        subquery.*,
        entity_dataframe.entity_timestamp,
        entity_dataframe.{{featureview.name}}__entity_row_unique_id
    FROM {{ featureview.name }}__subquery AS subquery
    INNER JOIN {{ featureview.name }}__entity_dataframe AS entity_dataframe
    ON TRUE
        AND subquery.event_timestamp <= entity_dataframe.entity_timestamp

        {% if featureview.ttl == 0 %}{% else %}
        AND subquery.event_timestamp >= entity_dataframe.entity_timestamp - {{ featureview.ttl }} * interval '1' second
        {% endif %}

        {% for entity in featureview.entities %}
        AND subquery.{{ entity }} = entity_dataframe.{{ entity }}
        {% endfor %}
),

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
        {{featureview.name}}__entity_row_unique_id,
        MAX(event_timestamp) AS event_timestamp
        {% if featureview.created_timestamp_column %}
            ,ANY_VALUE(created_timestamp) AS created_timestamp
        {% endif %}

    FROM {{ featureview.name }}__base
    {% if featureview.created_timestamp_column %}
        INNER JOIN {{ featureview.name }}__dedup
        USING ({{featureview.name}}__entity_row_unique_id, event_timestamp, created_timestamp)
    {% endif %}

    GROUP BY {{featureview.name}}__entity_row_unique_id
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
{{ featureview.name }}__cleaned AS (
    SELECT base.*
    FROM {{ featureview.name }}__base as base
    INNER JOIN {{ featureview.name }}__latest
    USING(
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp
        {% if featureview.created_timestamp_column %}
            ,created_timestamp
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}


{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT *
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) USING ({{featureview.name}}__entity_row_unique_id)
{% endfor %}
"""
