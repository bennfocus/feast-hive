# Feast Hive Support

Hive is not included in current [Feast](https://github.com/feast-dev/feast) roadmap, this project intends to add Hive support for Offline Store.  
For more details, can check [this Feast issue](https://github.com/feast-dev/feast/issues/1686).

**Important: This project is still being developed and not ready for using yet, I am going make a first workable release in a couple of days.**

**currently only supports `multiple insert` for uploading entity_df, which will be a little unefficient, going to provider another choice for users who are able to provider WebHDFS** 

## Quickstart

#### Install feast

```shell
pip install feast
```

#### Install feast-hive

Install the latest dev version by pip:

```shell
pip install git+https://github.com/baineng/feast-hive.git 
```

or by clone the repo:

```shell
git clone https://github.com/baineng/feast-hive.git
cd feast-hive
python setup.py install
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
    port: 10000 # default
    ... # other parameters
online_store:
    ...
```

#### Add `hive_example.py`

```python
# This is an example feature definition file

from google.protobuf.duration_pb2 import Duration

from feast import Entity, Feature, FeatureView, ValueType
from feast_hive import HiveSource

# Read data from Hive table
# Need make sure the table exists and have data before continue.
driver_hourly_stats = HiveSource(
    table='example.driver_stats',
    event_timestamp_column="datetime",
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
pytest --hs2_host=localhost --hs2_port=10000
```