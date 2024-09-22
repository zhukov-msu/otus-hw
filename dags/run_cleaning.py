from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.decorators import dag, task
import pendulum
from src.tools import *

@dag(
    schedule='*/3 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def cleaning_taskflow():
    @task
    def check_files_processed():
        session = boto3.session.Session(aws_access_key_id=KEY_ID, aws_secret_access_key=SECRET)
        s3 = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net'
        )
        filelist_input_bucket = list(set([
            key['Key'].split('.')[0]
            for key in s3.list_objects(Bucket=INPUT_BUCKET)['Contents']
        ]))
        filelist_result_bucket = list(set([
            key['Key'].split('.')[0]
            for key in s3.list_objects(Bucket=RESULT_BUCKET)['Contents']
        ]))
        session.close()
        files_not_processed = [f for f in filelist_input_bucket if not f in filelist_result_bucket]

    @task
    def check_files_processed():
        pass




