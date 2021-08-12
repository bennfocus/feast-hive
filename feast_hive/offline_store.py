import time
from datetime import datetime
from typing import List, Literal, Optional, Set, Union

import pandas
import pyarrow
from pydantic import StrictBool, StrictInt, StrictStr

from feast import FeatureView
from feast.data_source import DataSource
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast_hive.data_source import HiveSource

try:
    from impala.dbapi import connect
    from impala.hiveserver2 import HiveServer2Cursor, OperationalError

except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("hive", str(e))

_PANDAS_DTYPES_TO_HIVE = {
    "object": "STRING",
    "string": "STRING",
    "bool": "BOOLEAN",
    "datetime64[ns, UTC]": "TIMESTAMP",
    "datetime64[ns]": "TIMESTAMP",
    "float32": "FLOAT",
    "float64": "DOUBLE",
    "uint8": "SMALLINT",
    "uint16": "INT",
    "uint32": "BIGINT",
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INT",
    "int64": "BIGINT",
}
# be careful of duplicated hive types
_HIVE_TYPES_TO_PANDAS = {v: k for k, v in _PANDAS_DTYPES_TO_HIVE.items()}
_ENTITY_UPLOADING_CHUNK_SIZE = 10000

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

        table_ref = data_source.table_ref
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
            query = f"""
            SELECT {field_string}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {table_ref} t1
                WHERE {event_timestamp_column} BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
            ) t2
            WHERE _feast_row = 1
            """

            cursor = conn.cursor()
            # execute the job now, then check status in to_df or to_arrow
            cursor.execute_async(query)
            return HiveRetrievalJob(cursor)

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, HiveOfflineStoreConfig)

        # TODO check entity_df first
        if len(entity_df) == 0:
            pass

        expected_join_keys = _get_join_keys(project, feature_views, registry)

        with connect(**config.offline_store.dict(exclude={"type"})) as conn:
            cursor = conn.cursor()
            # table = _upload_entity_df(
            #     client=client,
            #     project=config.project,
            #     dataset_name=config.offline_store.dataset,
            #     dataset_project=client.project,
            #     entity_df=entity_df,
            # )


class HiveRetrievalJob(RetrievalJob):
    def __init__(self, cursor: _Cursor):
        self._cursor = cursor

    def to_df(self) -> pandas.DataFrame:
        self._wait_results()
        return _get_dataframe_from_hive_data(
            self._cursor.fetchall(), self._cursor.description
        )

    def to_arrow(self) -> pyarrow.Table:
        return pyarrow.Table.from_pandas(self.to_df())

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
                time.sleep(_get_sleep_interval(loop_start))
        except KeyboardInterrupt:
            print("Canceling query")
            cur.cancel_operation()
            raise


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


def _get_join_keys(
    project: str, feature_views: List[FeatureView], registry: Registry
) -> Set[str]:
    join_keys = set()
    for feature_view in feature_views:
        entities = feature_view.entities
        for entity_name in entities:
            entity = registry.get_entity(entity_name, project)
            join_keys.add(entity.join_key)
    return join_keys


def _create_entity_table(
    project: str, cursor: _Cursor, entity_df: Union[pandas.DataFrame, str],
) -> str:
    entity_table_ref = f"entity_df_{project}_{int(time.time())}"

    # cursor.execute('set hive.exec.temporary.table.storage = memory')
    if type(entity_df) is str:
        cursor.execute(f"CREATE TEMPORARY TABLE {entity_table_ref} AS ({entity_df})")
    elif isinstance(entity_df, pandas.DataFrame):
        _upload_entity_df_to_hive(entity_table_ref, cursor, entity_df)
    else:
        raise ValueError(
            f"The entity dataframe you have provided must be a Pandas DataFrame or HQL query, "
            f"but we found: {type(entity_df)} "
        )

    return entity_table_ref


def _upload_entity_df_to_hive(
    entity_table_ref: str, cursor: _Cursor, entity_df: Union[pandas.DataFrame, str],
) -> None:
    cols_and_types = []
    for column_name, pandas_dtype in entity_df.dtypes.iteritems():
        if not isinstance(column_name, str):
            raise TypeError("Column names must be strings.")
        hive_type = _PANDAS_DTYPES_TO_HIVE.get(str(pandas_dtype))
        # maybe better to infer types for unknown types
        if not hive_type:
            raise ValueError(
                f'Not supported pandas dtype "{pandas_dtype}" in entity_df'
            )
        cols_and_types.append((column_name, hive_type))
    # create hive table according to pandas columns and dtypes
    create_entity_table_sql = f"""
        CREATE TEMPORARY TABLE {entity_table_ref} (
          {', '.join([f'{col} {type}' for (col, type) in cols_and_types])}
        )
        """
    cursor.execute(create_entity_table_sql)

    def enclose_sql_value(data, type):
        if type in ["STRING", "TIMESTAMP"]:
            return f'"{data}"'
        else:
            return data

    # insert pandas data into the hive table
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
                    enclose_sql_value(str(entity_df.loc[j, col]), type)
                    for (col, type) in cols_and_types
                ]
            )

        insert_entity_chunk_sql = f"""
            INSERT INTO TABLE {entity_table_ref}
            VALUES ({'), ('.join([', '.join(entity_row) for entity_row in chunk_data])})
        """
        cursor.execute(insert_entity_chunk_sql)


def _get_dataframe_from_hive_data(data, cols_and_types) -> pandas.DataFrame:
    dtypes = {}
    for field in cols_and_types:
        if field[1] in _HIVE_TYPES_TO_PANDAS:
            dtypes[field[0]] = _HIVE_TYPES_TO_PANDAS[field[1]]

    df = pandas.DataFrame(data, columns=[field[0] for field in cols_and_types],)
    return df.astype(dtypes) if len(dtypes) > 0 else df
