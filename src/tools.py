import os

import findspark
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
import boto3
import pickle

hdfsdir = '/user/data'
target_S3 = 's3a://hw3-data-cleaning/'
INPUT_BUCKET = "otus-hw-bucket"
RESULT_BUCKET = "hw3-data-cleaning"

with open("bucket_cred.pkl", 'rb') as f:
    d = pickle.load(f)
    KEY_ID = d["KEY_ID"]
    SECRET = d["SECRET"]


def get_spark(app_name: str = "otus-hw"):
    findspark.init()
    findspark.find()
    conf = (
        SparkConf().setMaster("yarn").setAppName(app_name)
        .set("spark.executor.memory", "8g")
        .set("spark.driver.memory", "4g")
        .set("spark.sql.execution.arrow.pyspark.enabled", "true")
        .set("spark.executor.instances", 4)
        .set("spark.executor.cores", 2)
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


def check_file_exist(filename: str, bucket: str = RESULT_BUCKET) -> bool:
    session = boto3.session.Session(aws_access_key_id=KEY_ID, aws_secret_access_key=SECRET)
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net'
    )
    filelist_s3 = [
        key['Key'].split('.')[0]
        for key in s3.list_objects(Bucket=bucket)['Contents']
    ]
    session.close()
    if filename in filelist_s3:
        return True
    else:
        return False
