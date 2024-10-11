# import findspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *

hdfsdir = '/user/data'
target_S3 = 's3a://hw3-data-cleaning/'
INPUT_BUCKET = "otus-hw-bucket"
RESULT_BUCKET = "hw3-data-cleaning"

def get_spark(app_name: str = "otus-hw"):
    # findspark.init()
    # findspark.find()
    conf = (
        SparkConf().setMaster("yarn").setAppName(app_name)
        .set("spark.executor.memory", "6g")
        .set("spark.driver.memory", "4g")
        .set("spark.sql.execution.arrow.pyspark.enabled", "true")
        .set("spark.dynamicAllocation.enabled", "true")
        .set("spark.executor.cores", "4")
        .set("spark.dynamicAllocation.minExecutors", "1")
        .set("spark.dynamicAllocation.maxExecutors", "2")
        .set('spark.sql.repl.eagerEval.enabled', True)
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark


def get_schema() -> StructType:
    return StructType([
        StructField("transaction_id", LongType(), True),
        StructField("tx_datetime", TimestampType(), True),
        StructField("customer_id", LongType(), True),
        StructField("terminal_id", LongType(), True),
        StructField("tx_amount", DoubleType(), True),
        StructField("tx_time_seconds", LongType(), True),
        StructField("tx_time_days", LongType(), True),
        StructField("tx_fraud", LongType(), True),
        StructField("tx_fraud_scenario", LongType(), True)
    ])

def clean_dataframe(df):
    df = df.na.drop("any")  # очистка пустых значений
    df = df.dropDuplicates(['transaction_id'])
    return df


def main():
    spark = get_spark()
    schema = get_schema()

    filelist = ['2019-11-20']
    for filename in filelist:
        df = spark.read.schema(schema) \
            .option("comment", "#") \
            .option("header", False) \
            .csv(f's3a://{INPUT_BUCKET}/{filename}.txt')
            # .csv(f'{hdfsdir}/{filename}.txt')
        df = clean_dataframe(df)
        # write to one parquet
        (
            df
            .repartition(1)
            .write.parquet(f"s3a://{RESULT_BUCKET}/{filename}.parquet", mode="overwrite")
         )
    spark.stop()


if __name__ == "__main__":
    main()