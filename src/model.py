import os
import sys
import logging

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

hdfsdir = '/user/data'
S3_URL = 's3a://hw3-data-cleaning/'
BUCKET = "hw3-data-cleaning"
mlflow_uri = "http://130.193.53.137:8000"

import pyspark.sql.functions as f
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.pipeline import Pipeline
from pyspark.sql.types import FloatType, IntegerType
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from mlflow.tracking import MlflowClient

import mlflow
from datetime import datetime
from data_clean import get_schema, get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

from scipy.stats import shapiro
from scipy.stats import ttest_ind

BOOTSTRAP_ITERATIONS = 10

def main():
    spark = get_spark()

    logger.info("Read parquet")
    file_name = "2019-08-22.parquet"
    df = spark.read.parquet(f's3a://{BUCKET}/{file_name}').limit(20000)
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
    logger.info(f"df_1count: {df_1count}")
    logger.info(f"df_0count: {df_0count}")

    logger.info("Balancing samples by tx_fraud")
    df1Over = df_1 \
        .withColumn("dummy",
                    f.explode(
                        f.array(*[f.lit(x)
                                  for x in range(int(df_0count / df_1count))]))) \
        .drop("dummy")

    logger.info("Union all to data")
    data = df_0.unionAll(df1Over)

    logger.info("VectorAssembler, MinMaxScaler")

    numeric_cols = ["terminal_id", "hour_tx_datetime", "tx_amount", "percent_fraud_on_terminal"]
    featureColumns = numeric_cols #+ cat_cols

    assembler = VectorAssembler() \
        .setInputCols(featureColumns) \
        .setOutputCol("features") \
        .setHandleInvalid("skip")

    scaler = MinMaxScaler() \
        .setInputCol("features") \
        .setOutputCol("scaledFeatures")

    logger.info("Pipeline to data")

    scaled = Pipeline(stages=[
        assembler,
        scaler,
    ]).fit(data).transform(data)

    logger.info("Training and test samples, 80/20")
    tt = scaled.randomSplit([0.8, 0.2])
    training = tt[0]
    test = tt[1]

    logger.info("Mlflow is starting")
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
    mlflow.set_tracking_uri(mlflow_uri)

    experiment = mlflow.set_experiment("zhukov-test")
    experiment_id = experiment.experiment_id
    logger.info(f"Experiment ID: {experiment_id}")

    run_name = 'LogReg' + str(datetime.now())

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

        model = lr.fit(training)

        run_id = mlflow.active_run().info.run_id

        mlflow.log_param('optimal_regParam', setRegParam)
        mlflow.log_param('optimal_elasticNetParam', setElasticNetParam)

        logger.info("Training is starting")
        trainingSummary = model.summary
        accuracy_trainingSummary = trainingSummary.accuracy
        areaUnderROC_trainingSummary = trainingSummary.areaUnderROC

        mlflow.log_metric("accuracy_trainingSummary", accuracy_trainingSummary)
        mlflow.log_metric("areaUnderROC_trainingSummary", areaUnderROC_trainingSummary)

        # logger.info("tp, tn, fp, fn")
        # predicted = model.transform(test)
        # tp = predicted.filter((f.col("tx_fraud") == 1) & (f.col("prediction") == 1)).count()
        # tn = predicted.filter((f.col("tx_fraud") == 0) & (f.col("prediction") == 0)).count()
        # fp = predicted.filter((f.col("tx_fraud") == 0) & (f.col("prediction") == 1)).count()
        # fn = predicted.filter((f.col("tx_fraud") == 1) & (f.col("prediction") == 0)).count()
        #
        # logger.info("accuracy, precision, recall, Fmeasure")
        # accuracy = (tp + tn) / (tp + tn + fp + fn)
        # precision = tp / (tp + fp)
        # recall = tp / (tp + fn)
        # Fmeasure = 2 * recall * precision / (recall + precision)
        #
        # mlflow.log_metric("accuracy", accuracy)
        # mlflow.log_metric("precision", precision)
        # mlflow.log_metric("recall", recall)
        # mlflow.log_metric("Fmeasure", Fmeasure)

        mlflow.spark.save_model(model, "LrModelSaved")
        # mlflow.spark.log_model(model, "LrModelLogs")

        predictions = model.transform(test)
        current_metrics = []
        evaluator = BinaryClassificationEvaluator(labelCol="tx_fraud")

        # Running bootstrap samples evaluation
        logger.info("Evaluator is starting")
        for _ in range(BOOTSTRAP_ITERATIONS):
            sample = predictions.sample(withReplacement=True, fraction=0.2, seed=42)
            roc_auc = evaluator.evaluate(sample)

            mlflow.log_metric("roc_auc", roc_auc)
            current_metrics.append(roc_auc)

        logger.info("Evaluator is finished")

        # Mean of metrics
        roc_auc_mean = sum(current_metrics) / len(current_metrics)
        mlflow.log_metric("roc_auc_mean", roc_auc_mean)

        logger.info("Check previous runs")
        client = MlflowClient(tracking_uri=mlflow_uri)
        if len(client.search_runs([experiment_id], max_results=1)) < 1:
            is_first = True
        else:
            is_first = False
            best_run = client.search_runs(experiment_id, order_by=["metrics.roc_auc_mean DESC"], max_results=1)[0]

        logger.info(f"is_first: {is_first}")

        logger.info("log metrics to MLFlow")
        if is_first:
            mlflow.spark.log_model(model, "LrModelLogs")
        else:
            # if roc_auc_mean > best_run.data.metrics.get("roc_auc_mean", 0):
            best_run_id = best_run.info.run_id
            best_metrics = []

            for best_metric in client.get_metric_history(best_run_id, "roc_auc"):
                best_metrics.append(best_metric.value)

            pvalue = ttest_ind(best_metrics, current_metrics).pvalue
            mlflow.log_metric("p-value", pvalue)
            logger.info(f"p-value: {pvalue}")

            # If the new mean is significantly higher than the previous one, save the model
            alpha = 0.05
            if pvalue < alpha:
                mlflow.spark.log_model(model, "LrModelLogs")

    spark.stop()

    print("Done")


if __name__ == "__main__":
    main()