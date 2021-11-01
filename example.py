# This is an example feature definition file

from google.protobuf.duration_pb2 import Duration

from feast import Entity, Feature, FeatureView, ValueType
from feast_hive import HiveSource

# Read data from Hive table
# Here we use a Query to reuse the original parquet data,
# but you can replace to your own Table or Query.
driver_hourly_stats = HiveSource(
    # table='driver_stats',
    query = """
    SELECT from_unixtime(cast(event_timestamp / 1000000 as bigint)) AS event_timestamp, 
           driver_id, conv_rate, acc_rate, avg_daily_trips, 
           from_unixtime(cast(created / 1000000 as bigint)) AS created 
    FROM driver_stats
    """,
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)

# Define an entity for the driver.
driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id", )

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