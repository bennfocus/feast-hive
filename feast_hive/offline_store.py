import time
from datetime import datetime
from typing import List, Literal, Optional, Set, Union

import pandas
import pyarrow

from feast import FeatureView
from feast.data_source import DataSource
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from pydantic import StrictStr, StrictInt, StrictBool

from feast_hive.data_source import HiveSource

try:
    from impala.dbapi import connect
    from impala.hiveserver2 import HiveServer2Cursor

except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("hive", str(e))


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

    auth_mechanism: StrictStr = 'PLAIN'
    """ Specify the authentication mechanism. `'PLAIN'` for unsecured, `'GSSAPI'` for Kerberos and `'LDAP'` for Kerberos with
        LDAP. """

    user: Optional[StrictStr] = None
    """ LDAP user, if applicable. """

    password: Optional[StrictStr] = None
    """ LDAP password, if applicable. """

    use_http_transport: StrictBool = False
    """ Set it to True to use http transport of False to use binary transport. """

    http_path: StrictStr = ''
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

        with connect(**config.offline_store.dict(exclude={'type'})) as conn:
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

        expected_join_keys = _get_join_keys(project, feature_views, registry)

        # with connect(**config.offline_store.dict(exclude={'type'})) as conn:
        #     table = _upload_entity_df(
        #         client=client,
        #         project=config.project,
        #         dataset_name=config.offline_store.dataset,
        #         dataset_project=client.project,
        #         entity_df=entity_df,
        #     )


class HiveRetrievalJob(RetrievalJob):
    def __init__(self, cursor: HiveServer2Cursor):
        self.cursor = cursor

    def to_df(self) -> pandas.DataFrame:
        self._waiting_results()
        df = pandas.DataFrame(
            self.cursor.fetchall(),
            columns=[field[0] for field in self.cursor.description],
        )
        return df

    def to_arrow(self) -> pyarrow.Table:
        return pyarrow.Table.from_pandas(self.to_df())

    def _waiting_results(self):
        while self.cursor.is_executing():
            time.sleep(0.1)


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


def _upload_entity_df():
    pass