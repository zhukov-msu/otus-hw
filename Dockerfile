FROM python:3.10-slim-buster
LABEL authors="a.zhukov"

ENV APP_HOME=/app

WORKDIR $APP_HOME
ENV PYTHONPATH=$APP_HOME

COPY requirements.txt .

RUN pip install --upgrade pip setuptools wheel
RUN pip install fastapi numpy uvicorn scikit-learn starlette-exporter
#RUN pip install -r requirements.txt


COPY scripts scripts
RUN chmod +x scripts/*.sh
COPY src src

EXPOSE 8890
EXPOSE 8000
EXPOSE 80
EXPOSE 8080
EXPOSE 8090
WORKDIR ./src

CMD ["bash", "../scripts/entrypoint.sh"]