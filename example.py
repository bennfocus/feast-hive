# This is an example feature definition file

import datetime

from feast import Entity, FeatureView, Field, ValueType
from feast.types import PrimitiveFeastType
from feast_hive import HiveSource

# Read data from Hive table
# Here we use a Query to reuse the original parquet data,
# but you can replace to your own Table or Query.
driver_hourly_stats = HiveSource(
    # table='driver_stats',
    query="""
    SELECT from_unixtime(cast(event_timestamp / 1000000 as bigint)) AS event_timestamp, 
           driver_id, conv_rate, acc_rate, avg_daily_trips, 
           from_unixtime(cast(created / 1000000 as bigint)) AS created 
    FROM driver_stats
    """,
    name="driver_stats",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Define an entity for the driver.
driver = Entity(name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64, description="driver id", )

# Define FeatureView
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=datetime.timedelta(seconds=86400 * 1),
    schema=[
        Field(name="conv_rate", dtype=PrimitiveFeastType.FLOAT32),
        Field(name="acc_rate", dtype=PrimitiveFeastType.FLOAT32),
        Field(name="avg_daily_trips", dtype=PrimitiveFeastType.INT32),
    ],
    online=True,
    source=driver_hourly_stats,
    tags={},
)
