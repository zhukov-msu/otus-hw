import os
import sys
import logging

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
import pandas as pd
import mlflow
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression
from pyspark import SparkConf

from pyspark.sql import SparkSession
conf = (
        SparkConf().setMaster("yarn").setAppName('test')
        .set("spark.executor.memory", "6g")
        .set("spark.driver.memory", "4g")
        .set("spark.sql.execution.arrow.pyspark.enabled", "true")
        .set("spark.dynamicAllocation.enabled", "true")
        .set("spark.executor.cores", "4")
        .set("spark.dynamicAllocation.minExecutors", "1")
        .set("spark.dynamicAllocation.maxExecutors", "4")
        .set('spark.sql.repl.eagerEval.enabled', True)
    )
spark = SparkSession.builder.config(conf=conf).getOrCreate()
# Prepare training data from a list of (label, features) tuples.
train_df = spark.createDataFrame([
    (Vectors.dense([0.0, 1.1, 0.1]),1.0),
    (Vectors.dense([2.0, 1.0, -1.0]),0.0),
    (Vectors.dense([2.0, 1.3, 1.0]),0.0),
    (Vectors.dense([0.0, 1.2, -0.5]),1.0)], ["features", "label"])


lor = LogisticRegression(maxIter=2)
lor.setPredictionCol("").setProbabilityCol("prediction")
lor_model = lor.fit(train_df)

test_df = train_df.select("features")
prediction_df = lor_model.transform(train_df).select("prediction")

#signature = infer_signature(test_df, prediction_df)
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
mlflow.set_tracking_uri("http://158.160.2.210:8000")
from datetime import datetime
run_name = 'test_mf_' + str(datetime.now())
experiment = mlflow.set_experiment("zhukov-test-1")
experiment_id = experiment.experiment_id
with mlflow.start_run(run_name=run_name, experiment_id=experiment_id) as run:
    model_info = mlflow.spark.log_model(
        lor_model,
        "model",
        #signature=signature,
    )

print(model_info.signature)

loaded = mlflow.pyfunc.load_model(model_info.model_uri)

test_dataset = pd.DataFrame({"features": [[1.0, 2.0]]})

# `loaded.predict` accepts `Array[double]` type input column,
# and generates `Array[double]` type output column.
print(loaded.predict(test_dataset))
