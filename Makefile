SHELL = /bin/bash
airflow-cp:
	git pull
	cp ./dags/test.py /home/airflow/dags