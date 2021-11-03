import pickle
from typing import Callable, Dict, Iterable, Optional, Tuple

from feast import RepoConfig, ValueType
from feast.data_source import DataSource
from feast.errors import DataSourceNotFoundException
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
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
        table: Optional[str] = None,
        query: Optional[str] = None,
        event_timestamp_column: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
    ):
        assert (
            table is not None or query is not None
        ), '"table" or "query" is required for HiveSource.'

        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

        self._hive_options = HiveOptions(table=table, query=query)

    def __eq__(self, other):
        if not isinstance(other, HiveSource):
            raise TypeError("Comparisons should only involve HiveSource class objects.")

        return (
            self.hive_options.table == other.hive_options.table
            and self.hive_options.query == other.hive_options.query
            and self.event_timestamp_column == other.event_timestamp_column
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
            and self.date_partition_column == other.date_partition_column
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
            type=DataSourceProto.CUSTOM_SOURCE,
            field_mapping=self.field_mapping,
            custom_options=self.hive_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column
        return data_source_proto

    @staticmethod
    def from_proto(data_source: DataSourceProto):

        assert data_source.HasField("custom_options")

        hive_options = HiveOptions.from_proto(data_source.custom_options)

        return HiveSource(
            field_mapping=dict(data_source.field_mapping),
            table=hive_options.table,
            query=hive_options.query,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
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
                if self.table is not None:
                    table_splits = self.table.rsplit(".", 1)
                    database_name = table_splits[0] if len(table_splits) == 2 else None
                    table_name = (
                        table_splits[1] if len(table_splits) == 2 else table_splits[0]
                    )

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
