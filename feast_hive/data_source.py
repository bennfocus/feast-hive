from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from feast import RepoConfig, ValueType
from feast.data_source import DataSource
from feast.errors import DataSourceNotFoundException


class HiveOptions:
    """
    DataSource Hive options used to source features from Hive
    """

    def __init__(
        self,
        host: str,
        table_ref: str,
        port: Optional[int],
        extra_conn_params: Optional[Dict[str, Any]],
    ):
        self._host = host
        self._port = port
        self._table_ref = table_ref
        self._extra_conn_params = extra_conn_params

    @property
    def host(self):
        """
        Returns the hive connection host of this data source
        """
        return self._host

    @host.setter
    def host(self, host):
        """
        Sets the hive connection host of this data source
        """
        self._host = host

    @property
    def port(self):
        """
        Returns the hive connection port of this data source
        """
        return self._port

    @port.setter
    def port(self, port):
        """
        Sets the hive connection port of this data source
        """
        self._port = port

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

    @property
    def extra_conn_params(self):
        """
        Returns the extra connection parameters of this data source
        """
        return self._extra_conn_params

    @extra_conn_params.setter
    def extra_conn_params(self, extra_conn_params):
        """
        Sets the extra connection parameters of this data source
        """
        self._extra_conn_params = extra_conn_params

    @classmethod
    def from_proto(cls, hive_options_proto: Any):
        """
        Creates a HiveOptions from a protobuf representation of a hive option

        Args:
            hive_options_proto: A protobuf representation of a DataSource

        Returns:
            Returns a HiveOptions object based on the hive_options protobuf
        """

        pass

    def to_proto(self) -> None:
        """
        Converts an HiveOptionsProto object to its protobuf representation.

        Returns:
            HiveOptionsProto protobuf
        """

        pass


class HiveSource(DataSource):
    def __init__(
        self,
        host: str,
        table_ref: str,
        port: Optional[int] = 10000,
        extra_conn_params: Optional[Dict[str, Any]] = None,
        event_timestamp_column: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
    ):
        """Connect to HiveServer2

        :param host: What host HiveServer2 runs on.
        :param port: What port HiveServer2 runs on. Defaults to 10000.
        :param table_ref: The table ref of the data source.
        :param extra_conn_params: Extra connection params besides the host and port.
            Check here for the complete params list: https://github.com/cloudera/impyla/blob/255b07ed973d47a3395214ed92d35ec0615ebf62/impala/dbapi.py#L40
        :param event_timestamp_column:
        :param created_timestamp_column:
        :param field_mapping:
        :param date_partition_column:
        """

        assert host is not None and host != "", '"host" is required for HiveSource'
        assert (
            table_ref is not None and table_ref != ""
        ), '"table_ref" is required for HiveSource'

        _default_extra_conn_params = {"auth_mechanism": "PLAIN"}
        _extra_conn_params = (
            {**_default_extra_conn_params, **extra_conn_params}
            if extra_conn_params is not None
            else _default_extra_conn_params
        )

        self._hive_options = HiveOptions(
            host=host,
            port=port,
            table_ref=table_ref,
            extra_conn_params=_extra_conn_params,
        )

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
            self.hive_options.host == other.hive_options.host
            and self.hive_options.port == other.hive_options.host
            and self.hive_options.table_ref == other.hive_options.table_ref
            and self.hive_options.extra_conn_params
            == other.hive_options.extra_conn_params
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
    def host(self):
        return self._hive_options.host

    @property
    def port(self):
        return self._hive_options.port

    @property
    def table_ref(self):
        return self._hive_options.table_ref

    @property
    def extra_conn_params(self):
        return self._hive_options.extra_conn_params

    @property
    def all_conn_params(self):
        return {"host": self.host, "port": self.port, **self.extra_conn_params}

    def to_proto(self) -> None:
        pass

    def validate(self, config: RepoConfig):
        from impala.dbapi import connect

        with connect(**self.all_conn_params) as conn:
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
        return _hive_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        from impala.dbapi import connect

        with connect(**self.all_conn_params) as conn:
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


def _hive_to_feast_value_type(hive_type_as_str: str):
    type_map: Dict[str, ValueType] = {
        # Numeric Types
        "TINYINT": ValueType.INT32,
        "SMALLINT": ValueType.INT32,
        "INT": ValueType.INT32,
        "INTEGER": ValueType.INT32,
        "BIGINT": ValueType.INT64,
        "FLOAT": ValueType.DOUBLE,
        "DOUBLE": ValueType.DOUBLE,
        "DECIMAL": ValueType.STRING,
        "NUMERIC": ValueType.STRING,
        # Date/Time Types
        "TIMESTAMP": ValueType.STRING,
        "DATE": ValueType.STRING,
        "INTERVAL": ValueType.STRING,
        # String Types
        "STRING": ValueType.STRING,
        "VARCHAR": ValueType.STRING,
        "CHAR": ValueType.STRING,
        # Misc Types
        "BOOLEAN": ValueType.BOOL,
        "BINARY": ValueType.BYTES,
        # Complex Types
        "MAP": ValueType.STRING,
        "ARRAY": ValueType.STRING,
        "STRUCT": ValueType.STRING,
        "UNIONTYPE": ValueType.STRING,
        "NULL": ValueType.STRING,
    }
    return type_map[hive_type_as_str]
