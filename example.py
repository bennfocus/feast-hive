# This is an example feature definition file

from google.protobuf.duration_pb2 import Duration

from feast import Entity, Feature, FeatureView, ValueType
from feast_hive import HiveSource

# Read data from Hive table
# Need make sure the table_ref exists and have data before continue.
driver_hourly_stats = HiveSource(
    table='example.driver_stats',
    event_timestamp_column="datetime",
    created_timestamp_column="created",
)

# Define an entity for the driver.
driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id",)

# Define FeatureView
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=["driver_id"],
    ttl=Duration(seconds=86400 * 1),
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    online=True,
    input=driver_hourly_stats,
    tags={},
)
