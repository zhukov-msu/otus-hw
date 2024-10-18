from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

with DAG(
        dag_id = 'RUN_MODEL_FIT',
        schedule_interval='@daily',
        start_date=datetime(year = 2024,month = 1,day = 20),
        catchup=False
) as dag:
    t1 = BashOperator(
        task_id = 'run_model_fit',
        bash_command = 'ssh -i /home/ubuntu/.ssh/cluster ubuntu@10.1.0.27 \
         "spark-submit /home/ubuntu/otus-hw/src/model.py"',
    )

    # t1 = SSHOperator(
    #     task_id='fit_model',
    #     ssh_conn_id='mlops_ssh',
    #     command="spark-submit /home/ubuntu/otus-hw/src/",
    #     cmd_timeout=None,
    #     remote_host="10.1.0.27",
    #     key_file="/home/ubuntu/.ssh/yc_mlops",
    #     username="ubuntu"
    # )
