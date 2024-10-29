FROM python:3.8.10-slim-buster
LABEL authors="a.zhukov"

ENV APP_HOME=/app

WORKDIR $APP_HOME
ENV PYTHONPATH=$APP_HOME

COPY requirements.txt .

RUN pip install --upgrade pip setuptools wheel
RUN pip install -r requirements.txt

COPY scripts scripts
RUN chmod +x scripts/*.sh

COPY src src

EXPOSE 8890

CMD ["bash", "scripts/entrypoint.sh"]