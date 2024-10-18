import os
import sys
import logging

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

hdfsdir = '/user/data'
S3_URL = 's3a://hw3-data-cleaning/'
BUCKET = "hw3-data-cleaning"

# import findspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.pipeline import Pipeline
from pyspark.sql.types import FloatType, IntegerType

import mlflow
from datetime import datetime
from data_clean import get_schema, get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def main():
    spark = get_spark()

    logger.info("Read parquet")
    file_name = "2019-09-21.parquet"
    df = spark.read.parquet(f's3a://{BUCKET}/{file_name}')
    df_validation = (
        df
        .filter(f.col("tx_amount") > 0)
        .filter(f.col("customer_id") > 0)
    )

    logger.info(f"Starting remobe null from {file_name}")

    logger.info(f"Changing column types {file_name}")
    df_validation = df_validation.withColumn('tx_amount', df_validation['tx_amount'].cast(FloatType()))
    df_validation = df_validation.withColumn('terminal_id', df_validation['terminal_id'].cast(IntegerType()))
    df_validation = df_validation.withColumn('tx_fraud', df_validation['tx_fraud'].cast(IntegerType()))
    df_validation = df_validation.withColumn("tx_datetime", f.to_timestamp("tx_datetime", 'yyyy-MM-dd HH:mm:ss'))
    df_validation = df_validation.withColumn('hour_tx_datetime', f.hour(df_validation.tx_datetime))
    df_validation = df_validation.withColumn("tx_datetime", f.from_unixtime(f.unix_timestamp(df_validation.tx_datetime),
                                                                            'yyyy-MM-dd HH:mm:ss'))
    logger.info("Generating a pecent fraud on terminal")

    logger.info("df_cnt_transaction_on_terminal")
    df_cnt_transaction_on_terminal = (
        df_validation
        .select("terminal_id")
        .groupBy("terminal_id")
        .count()
        .withColumnRenamed("count", "cnt_transactions_on_terminal_id")
    )

    logger.info("df_cnt_fraud_transaction_on_terminal")
    df_cnt_fraud_transaction_on_terminal = (
        df_validation
        .filter("tx_fraud = 1")
        .select("terminal_id")
        .groupBy("terminal_id")
        .count()
        .withColumnRenamed("count", "cnt_fraud_transactions_on_terminal_id")
    )

    logger.info("df_fraud_transaction")
    df_fraud_transaction = df_cnt_transaction_on_terminal. \
        join(df_cnt_fraud_transaction_on_terminal, "terminal_id", "left")

    logger.info("column percent_fraud_on_terminal")

    df_fraud_transaction = df_fraud_transaction.withColumn("percent_fraud_on_terminal", f.round(
        f.col('cnt_fraud_transactions_on_terminal_id') / f.col('cnt_transactions_on_terminal_id') * 100, 2))
    df_fraud_transaction = df_fraud_transaction.withColumn('percent_fraud_on_terminal',
                                                           df_fraud_transaction['percent_fraud_on_terminal'].cast(
                                                               FloatType()))


    logger.info("Join df_fraud_transaction to df_validation")
    df_validation = df_validation.join(df_fraud_transaction, "terminal_id", "left")

    df = df_validation


    logger.info("Checking balance")
    df_1 = df.filter(df["tx_fraud"] == 1)
    df_0 = df.filter(df["tx_fraud"] == 0)

    df_1count = df_1.count()
    df_0count = df_0.count()

    logger.info("Balancing samples by tx_fraud")
    df1Over = df_1 \
        .withColumn("dummy",
                    f.explode(
                        f.array(*[f.lit(x)
                                  for x in range(int(df_0count / df_1count))]))) \
        .drop("dummy")

    logger.info("Union all to data")
    data = df_0.unionAll(df1Over)

    logger.info("StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler")
    data_indexer = StringIndexer(inputCols=["tx_fraud_scenario"],
                                 outputCols=["PercFraudIndex", "FraudScenarioIndex"])
    data_encoder = OneHotEncoder(inputCols=["PercFraudIndex", "FraudScenarioIndex"],
                                 outputCols=["PercFraudEncoded", "FraudScenarioEncoded"])

    numeric_cols = ["terminal_id", "hour_tx_datetime", "tx_amount"]
    cat_cols = ["PercFraudEncoded", "FraudScenarioEncoded"]
    featureColumns = numeric_cols + cat_cols

    assembler = VectorAssembler() \
        .setInputCols(featureColumns) \
        .setOutputCol("features") \
        .setHandleInvalid("skip")

    scaler = MinMaxScaler() \
        .setInputCol("features") \
        .setOutputCol("scaledFeatures")

    logger.info("Pipeline to data")

    scaled = Pipeline(stages=[
        data_indexer,
        data_encoder,
        assembler,
        scaler,
    ]).fit(data).transform(data)

    logger.info("Training and test samples, 80/20")
    tt = scaled.randomSplit([0.8, 0.2])
    training = tt[0]
    test = tt[1]

    logger.info("Mlflow is starting")
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
    mlflow.set_tracking_uri("http://158.160.2.210:8000")

    experiment = mlflow.set_experiment("zhukov-test")
    experiment_id = experiment.experiment_id

    run_name = 'My run name' + 'LogReg' + str(datetime.now())

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
        setRegParam = 0.2
        setElasticNetParam = 0.8

        logger.info("LogisticRegression is starting")
        lr = LogisticRegression() \
            .setMaxIter(1) \
            .setRegParam(setRegParam) \
            .setElasticNetParam(setElasticNetParam) \
            .setFamily("binomial") \
            .setFeaturesCol("scaledFeatures") \
            .setLabelCol("tx_fraud")

        lrModel = lr.fit(training)

        run_id = mlflow.active_run().info.run_id

        mlflow.log_param('optimal_regParam', setRegParam)
        mlflow.log_param('optimal_elasticNetParam', setElasticNetParam)

        logger.info("Training is starting")
        trainingSummary = lrModel.summary
        accuracy_trainingSummary = trainingSummary.accuracy
        areaUnderROC_trainingSummary = trainingSummary.areaUnderROC

        mlflow.log_metric("accuracy_trainingSummary", accuracy_trainingSummary)
        mlflow.log_metric("areaUnderROC_trainingSummary", areaUnderROC_trainingSummary)

        logger.info("Predicted is starting")
        predicted = lrModel.transform(test)

        logger.info("tp, tn, fp, fn")
        tp = predicted.filter((f.col("tx_fraud") == 1) & (f.col("prediction") == 1)).count()
        tn = predicted.filter((f.col("tx_fraud") == 0) & (f.col("prediction") == 0)).count()
        fp = predicted.filter((f.col("tx_fraud") == 0) & (f.col("prediction") == 1)).count()
        fn = predicted.filter((f.col("tx_fraud") == 1) & (f.col("prediction") == 0)).count()

        logger.info("accuracy, precision, recall, Fmeasure")
        accuracy = (tp + tn) / (tp + tn + fp + fn)
        precision = tp / (tp + fp)
        recall = tp / (tp + fn)
        Fmeasure = 2 * recall * precision / (recall + precision)

        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("Fmeasure", Fmeasure)

        mlflow.spark.save_model(lrModel, "LrModelSaved")
        mlflow.spark.log_model(lrModel, "LrModelLogs")

    spark.stop()

    print("Done")


if __name__ == "__main__":
    main()