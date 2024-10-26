from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

with DAG(
        dag_id = 'RUN_MODEL_FIT',
        schedule_interval='@yearly',
        start_date=datetime(year = 2024,month = 1,day = 20),
        catchup=False
) as dag:
    t1 = BashOperator(
        task_id = 'run_model_fit',
        bash_command = 'ssh -i /home/airflow/id_rsa ubuntu@10.1.0.15 \
         "spark-submit --jars /home/ubuntu/mlflow-spark-1.27.0.jar /home/ubuntu/otus-hw/src/model.py"',
        # run_as_user='ubuntu'
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
