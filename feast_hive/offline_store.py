from datetime import datetime
from typing import List, Optional, Union, Literal, Set

import pandas
import pyarrow
from feast import FeatureView
from feast.data_source import DataSource
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig

from .data_source import HiveSource

try:
    from pyhive import hive
    from TCLIService.ttypes import TOperationState

except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("hive", str(e))


class HiveOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for Hive """

    type: Literal["hive"] = "hive"
    """ Offline store type selector"""


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

        with hive.connect(**data_source.pyhive_conn_params) as conn:
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

            cursor = conn.cursor()
            # execute the job now, then check status in to_df or to_arrow
            cursor.execute(query, async_=True)
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


class HiveRetrievalJob(RetrievalJob):
    def __init__(self, cursor: hive.Cursor):
        self.cursor = cursor

    def to_df(self):
        self._waiting_results()
        df = pandas.DataFrame(
            self.cursor.fetchall(),
            columns=[field[0] for field in self.cursor.description]
        )
        return df

    def to_arrow(self) -> pyarrow.Table:
        return pyarrow.Table.from_pandas(self.to_df())

    def _waiting_results(self):
        status = self.cursor.poll().operationState
        while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
            status = self.cursor.poll().operationState


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
