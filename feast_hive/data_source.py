from typing import Optional, Dict, Callable, Iterable, Tuple

from feast import RepoConfig, ValueType, type_map
from feast.data_source import DataSource
from feast.errors import DataSourceNotFoundException


class HiveSource(DataSource):
    def __init__(
            self,
            event_timestamp_column: Optional[str] = "",
            table_ref: Optional[str] = None,
            created_timestamp_column: Optional[str] = "",
            field_mapping: Optional[Dict[str, str]] = None,
            date_partition_column: Optional[str] = "",
            query: Optional[str] = None,
    ):
        self._bigquery_options = BigQueryOptions(table_ref=table_ref, query=query)

        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

    def __eq__(self, other):
        if not isinstance(other, HiveSource):
            raise TypeError(
                "Comparisons should only involve HiveSource class objects."
            )

        return (
                self.bigquery_options.table_ref == other.bigquery_options.table_ref
                and self.bigquery_options.query == other.bigquery_options.query
                and self.event_timestamp_column == other.event_timestamp_column
                and self.created_timestamp_column == other.created_timestamp_column
                and self.field_mapping == other.field_mapping
        )

    @property
    def table_ref(self):
        return self._bigquery_options.table_ref

    @property
    def query(self):
        return self._bigquery_options.query

    @property
    def bigquery_options(self):
        """
        Returns the bigquery options of this data source
        """
        return self._bigquery_options

    @bigquery_options.setter
    def bigquery_options(self, bigquery_options):
        """
        Sets the bigquery options of this data source
        """
        self._bigquery_options = bigquery_options

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_BIGQUERY,
            field_mapping=self.field_mapping,
            bigquery_options=self.bigquery_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        if not self.query:
            from google.api_core.exceptions import NotFound
            from google.cloud import bigquery

            client = bigquery.Client()
            try:
                client.get_table(self.table_ref)
            except NotFound:
                raise DataSourceNotFoundException(self.table_ref)

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self.table_ref:
            return f"`{self.table_ref}`"
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.bq_to_feast_value_type

    def get_table_column_names_and_types(
            self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        from google.cloud import bigquery

        client = bigquery.Client()
        if self.table_ref is not None:
            table_schema = client.get_table(self.table_ref).schema
            if not isinstance(table_schema[0], bigquery.schema.SchemaField):
                raise TypeError("Could not parse BigQuery table schema.")

            name_type_pairs = [(field.name, field.field_type) for field in table_schema]
        else:
            bq_columns_query = f"SELECT * FROM ({self.query}) LIMIT 1"
            queryRes = client.query(bq_columns_query).result()
            name_type_pairs = [
                (schema_field.name, schema_field.field_type)
                for schema_field in queryRes.schema
            ]

        return name_type_pairs
