import pickle
from typing import Callable, Dict, Iterable, Optional, Tuple

from feast import RepoConfig, ValueType
from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException
from feast.errors import DataSourceNotFoundException
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.saved_dataset import SavedDatasetStorage
from feast_hive.hive_type_map import hive_to_feast_value_type


class HiveOptions:
    """
    DataSource Hive options used to source features from Hive
    """

    def __init__(self, table: Optional[str], query: Optional[str]):
        self._table = table
        self._query = query

    @property
    def query(self):
        """
        Returns the query referenced by this source
        """
        return self._query

    @query.setter
    def query(self, query):
        """
        Sets the query referenced by this source
        """
        self._query = query

    @property
    def table(self):
        """
        Returns the table ref of this data source
        """
        return self._table

    @table.setter
    def table(self, table):
        """
        Sets the table ref of this data source
        """
        self._table = table

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
            table=hive_configuration.table, query=hive_configuration.query
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
        *,
        name: Optional[str] = None,
        timestamp_field: Optional[str] = None,
        query: Optional[str] = None,
        table: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        assert (
            table is not None or query is not None
        ), '"table" or "query" is required for HiveSource.'

        self._hive_options = HiveOptions(table=table, query=query)

        # If no name, use the table as the default name
        _name = name
        if not _name:
            if table:
                _name = table
            else:
                raise DataSourceNoNameException()

        super().__init__(
            name=_name if _name else "",
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            date_partition_column=date_partition_column,
            description=description,
            tags=tags,
            owner=owner,
        )

    # Note: Python requires redefining hash in child classes that override __eq__
    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, HiveSource):
            raise TypeError("Comparisons should only involve HiveSource class objects.")

        return (
            self.name == other.name
            and self.hive_options.table == other.hive_options.table
            and self.hive_options.query == other.hive_options.query
            and self.timestamp_field == other.timestamp_field
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
            and self.date_partition_column == other.date_partition_column
            and self.description == other.description
            and self.tags == other.tags
            and self.owner == other.owner
        )

    @property
    def table(self):
        return self._hive_options.table

    @property
    def query(self):
        return self._hive_options.query

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

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            field_mapping=self.field_mapping,
            custom_options=self.hive_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column
        return data_source_proto

    @staticmethod
    def from_proto(data_source: DataSourceProto):

        assert data_source.HasField("custom_options")

        hive_options = HiveOptions.from_proto(data_source.custom_options)

        return HiveSource(
            name=data_source.name,
            field_mapping=dict(data_source.field_mapping),
            table=hive_options.table,
            query=hive_options.query,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def validate(self, config: RepoConfig):
        self.get_table_column_names_and_types(config)

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self.table:
            return f"`{self.table}`"
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return hive_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        from impala.error import HiveServer2Error

        from feast_hive.hive import HiveConnection

        with HiveConnection(config.offline_store) as conn:
            with conn.cursor() as cursor:
                database_name = config.offline_store.database if config.offline_store.database else 'default'
                if self.table is not None:
                    table_splits = self.table.rsplit(".", 1)
                    if len(table_splits) == 1:
                        table_name = table_splits[0]
                    elif len(table_splits) == 2:
                        database_name = table_splits[0] if len(table_splits) == 2 else None
                        table_name = table_splits[1]
                    else:
                        raise ValueError(f"Invalid table name {self.table}, "
                                         f"table name should be [db].[tbl] or just [tbl]")
                    try:
                        return cursor.get_table_schema(table_name, database_name)
                    except:
                        raise DataSourceNotFoundException(self.table)

                else:
                    try:
                        cursor.execute(f"SELECT * FROM ({self.query}) AS t LIMIT 1")
                        if not cursor.fetchone():
                            raise DataSourceNotFoundException(self.query)

                        return [(field[0], field[1]) for field in cursor.description]
                    except HiveServer2Error:
                        raise DataSourceNotFoundException(self.query)


class SavedDatasetHiveStorage(SavedDatasetStorage):
    _proto_attr_name = "custom_storage"

    hive_options: HiveOptions

    def __init__(self, table: Optional[str] = None):
        self.hive_options = HiveOptions(table=table, query=None)

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        return SavedDatasetHiveStorage(
            table=HiveOptions.from_proto(storage_proto.custom_storage).table,
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(
            custom_storage=self.hive_options.to_proto()
        )

    def to_data_source(self) -> DataSource:
        return HiveSource(table=self.hive_options.table)
