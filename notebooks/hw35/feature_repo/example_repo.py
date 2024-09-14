# This is an example feature definition file

from datetime import timedelta

import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    RequestSource,
)
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64

customer = Entity(name="customer", join_keys=["customer_id"])

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_stats_source = FileSource(
    name="driver_hourly_stats_source",
    path="/Users/a.zhukov/dev/botva/MLOps/otus-hw/notebooks/hw35/feature_repo/data/sales.parquet",
    timestamp_field="order_item_work_dt",
    created_timestamp_column="order_item_create_dt",
)

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
sales_daily_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="sales_daily_stats",
    entities=[customer],
    ttl=timedelta(days=1),
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    schema=[
        Field(name="order_qty_cnt", dtype=Int64),
        Field(name="order_cost_with_tax_amt", dtype=Float32),
    ],
    online=True,
    source=driver_stats_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    # tags={"team": "driver_performance"},
)

sales_hourly_fv = FeatureView(
    name="sales_hourly_stats",
    entities=[customer],
    ttl=timedelta(hours=1),
    schema=[
        Field(name="order_qty_cnt", dtype=Int64),
        Field(name="order_cost_with_tax_amt", dtype=Float32),
    ],
    online=True,
    source=driver_stats_source,
    # tags={"team": "driver_performance"},
)

# Define a request data source which encodes features / information only
# available at request time (e.g. part of the user initiated HTTP request)
input_request = RequestSource(
    name="tax_value",
    schema=[
        Field(name="tax_value", dtype=Float32),
    ],
)


# Define an on demand feature view which can generate new features based on
# existing feature views and RequestSource features
@on_demand_feature_view(
    sources=[sales_daily_fv, input_request],
    schema=[
        Field(name="order_cost_without_tax_amt", dtype=Float64),
    ],
)
def no_tax_daily(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["order_cost_without_tax_amt"] = inputs["order_cost_with_tax_amt"] * (1 - inputs["tax_value"])
    return df


# This groups features into a model version
sales_daily_v1 = FeatureService(
    name="sales_daily_v1",
    features=[
        sales_daily_fv[["order_qty_cnt"]],  # Sub-selects a feature from a feature view
        no_tax_daily[["order_cost_without_tax_amt"]]
    ],
    logging_config=LoggingConfig(
        destination=FileLoggingDestination(path="/Users/a.zhukov/dev/botva/MLOps/otus-hw/notebooks/hw35/feature_repo/data")
    ),
)
