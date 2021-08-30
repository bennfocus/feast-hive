# Feast Hive Support

Hive is not included in current [Feast](https://github.com/feast-dev/feast) roadmap, this project intends to add Hive support for Offline Store.  
For more details, can check [this Feast issue](https://github.com/feast-dev/feast/issues/1686).

## Important

**The first stable version (v0.1.0) has been published, please create an issue if you have met any problem.**

#### Todo & Progress
- [DONE] ~~I am working on the first workable version, think it will be released in a couple of days.~~
- It only supports `insert into` for uploading entity_df for now, which will be a little inefficient. In next version, I will provide some extra parameters for users who are able to provide WebHDFS address. 

## Quickstart

#### Install feast

```shell
pip install feast
```

#### Install feast-hive

- Install stable version

```shell
pip install feast-hive 
```

- Install develop version (not stable):

```shell
pip install git+https://github.com/baineng/feast-hive.git 
```

#### Create a feature repository

```shell
feast init feature_repo
cd feature_repo
```

#### Edit `feature_store.yaml`

set `offline_store` type to be `feast_hive.HiveOfflineStore`

```yaml
project: ...
registry: ...
provider: local
offline_store:
    type: feast_hive.HiveOfflineStore
    host: localhost
    port: 10000        # default is `10000`
    database: default  # default is `default`
    ... # other parameters
online_store:
    ...
```

#### Create Hive Table

1. Upload `data/driver_stats.parquet` to HDFS
```shell
hdfs dfs -copyFromLocal ./data/driver_stats.parquet /tmp/
```
2. Create Hive Table
```sql
CREATE TABLE driver_stats (
    event_timestamp   bigint,
    driver_id         bigint,
    conv_rate         float,
    acc_rate          float,
    avg_daily_trips   int,
    created           bigint
)
STORED AS PARQUET;
```
3. Load data into the table
```sql
LOAD DATA INPATH '/tmp/driver_stats.parquet' INTO TABLE driver_stats;
```

#### Edit `example.py`

```python
# This is an example feature definition file

from google.protobuf.duration_pb2 import Duration

from feast import Entity, Feature, FeatureView, ValueType
from feast_hive import HiveSource

# Read data from Hive table
# Here we use a Query to reused the original parquet data, but you can replace to your own Table or Query.
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
```

#### Apply the feature definitions

```shell
feast apply
```

#### Generating training data and so on

The rest are as same as [Feast Quickstart](https://docs.feast.dev/quickstart#generating-training-data)


## Developing and Testing

#### Developing

```shell
git clone https://github.com/baineng/feast-hive.git
cd feast-hive
# creating virtual env ...
pip install -e .[dev]

# before commit
make format
makr lint
```

#### Testing

```shell
pip install -e .[test]
pytest -n 6 --host=localhost --port=10000 --database=default
```