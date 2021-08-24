from typing import Any, Callable, Dict, Iterable, Optional, Tuple

import pickle
from feast import RepoConfig, ValueType
from feast.data_source import DataSource
from feast.errors import DataSourceNotFoundException
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast_hive.type_map import hive_to_feast_value_type


class HiveOptions:
    """
    DataSource Hive options used to source features from Hive
    """

    def __init__(
        self, table_ref: str,
    ):
        self._table_ref = table_ref

    @property
    def table_ref(self):
        """
        Returns the table ref of this data source
        """
        return self._table_ref

    @table_ref.setter
    def table_ref(self, table_ref):
        """
        Sets the table ref of this data source
        """
        self._table_ref = table_ref

    @classmethod
    def from_proto(cls, hive_options_proto: DataSourceProto.CustomSourceOptions):
        """
        Creates a HiveOptions from a protobuf representation of a hive option

        Args:
            hive_options_proto: A protobuf representation of a DataSource

        Returns:
            Returns a HiveOptions object based on the hive_options protobuf
        """
        hive_configuration = pickle.loads(hive_options_proto.configuration)

        hive_options = cls(
            table_ref=hive_configuration.table_ref
        )

        return hive_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        """
        Converts an HiveOptionsProto object to its protobuf representation.

        Returns:
            HiveOptionsProto protobuf
        """
        hive_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=pickle.dumps(self)
        )
        return hive_options_proto



class HiveSource(DataSource):
    def __init__(
        self,
        table_ref: str,
        event_timestamp_column: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
    ):
        """Connect to HiveServer2

        :param table_ref: The table ref (table name or view name) in Hive.
        :param event_timestamp_column:
        :param created_timestamp_column:
        :param field_mapping:
        :param date_partition_column:
        """
        assert (
            table_ref is not None and table_ref != ""
        ), '"table_ref" is required for HiveSource'

        self._hive_options = HiveOptions(table_ref=table_ref)
        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

    def __eq__(self, other):
        if not isinstance(other, HiveSource):
            raise TypeError("Comparisons should only involve HiveSource class objects.")

        return (
            self.hive_options.table_ref == other.hive_options.table_ref
            and self.event_timestamp_column == other.event_timestamp_column
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
            and self.date_partition_column == other.date_partition_column
        )

    @property
    def hive_options(self):
        """
        Returns the hive options of this data source
        """
        return self._hive_options

    @hive_options.setter
    def hive_options(self, hive_options):
        """
        Sets the bigquery options of this data source
        """
        self._hive_options = hive_options

    @property
    def table_ref(self):
        return self._hive_options.table_ref

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.CUSTOM_SOURCE,
            field_mapping=self.field_mapping,
            custom_options=self.hive_options.to_proto()
        )
        
        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column
        return data_source_proto

    def from_proto(data_source:DataSourceProto):

        assert data_source.HasField("custom_options")

        return HiveSource(
            field_mapping=dict(data_source.field_mapping),
            table_ref=HiveOptions.from_proto(data_source.custom_options).table_ref,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
        )

    def validate(self, config: RepoConfig):
        from feast_hive.offline_store import HiveOfflineStoreConfig

        assert isinstance(config.offline_store, HiveOfflineStoreConfig)

        from impala.dbapi import connect
        #from pyhive import hive

        with connect(**config.offline_store.dict(exclude={"type"})) as conn:
            cursor = conn.cursor()
            table_ref_splits = self.table_ref.rsplit(".", 1)
            if len(table_ref_splits) == 2:
                cursor.execute(f"use {table_ref_splits[0]}")
                table_ref_splits.pop(0)
            cursor.execute(f'show tables like "{table_ref_splits[0]}"')
            if not cursor.fetchone():
                raise DataSourceNotFoundException(self.table_ref)

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return hive_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        from feast_hive.offline_store import HiveOfflineStoreConfig

        assert isinstance(config.offline_store, HiveOfflineStoreConfig)

        from impala.dbapi import connect

        with connect(**config.offline_store.dict(exclude={"type"})) as conn:
            cursor = conn.cursor()
            cursor.execute(f"desc {self.table_ref}")
            name_type_pairs = []
            for field in cursor.fetchall():
                if field[0] == "":
                    break
                name_type_pairs.append(
                    (field[0], str(field[1]).upper().split("<", 1)[0])
                )

            return name_type_pairs
