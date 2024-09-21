FROM apache/airflow:2.10.2-python3.8
LABEL authors="a.zhukov"

ENTRYPOINT ["top", "-b"]