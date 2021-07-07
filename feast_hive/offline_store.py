from datetime import datetime
from typing import List, Optional, Union, Literal

import pandas
from feast import FeatureView
from feast.data_source import DataSource
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig

try:
    from pyhive import hive

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
        pass

    @staticmethod
    def get_historical_features(
            config: RepoConfig,
            feature_views: List[FeatureView],
            feature_refs: List[str],
            entity_df: Union[pandas.DataFrame, str],
            registry: Registry,
            project: str,
    ) -> RetrievalJob:
        pass
