from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.ssh.operators.ssh import SSHOperator

with DAG(
        dag_id = 'RUN_MODEL_FIT',
        schedule_interval='@hourly',
        start_date=datetime(year = 2024,month = 1,day = 20),
        catchup=False
) as dag:
    t1 = SSHOperator(
        task_id='fit_model',
        ssh_conn_id='mlops_ssh',
        command="spark-submit /home/ubuntu/otus-hw/src/",
        cmd_timeout=None,
        remote_host="10.1.0.27",
        key_file="/home/ubuntu/.ssh/yc_mlops",
        username="ubuntu"
    )
