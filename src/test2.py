from mlflow.tracking import MlflowClient

import mlflow
import os

os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
mlflow.set_tracking_uri("http://130.193.53.137:8000")

experiment = mlflow.set_experiment("zhukov-test")
experiment_id = experiment.experiment_id
# logger.info(f"Experiment ID: {experiment_id}")
client = MlflowClient(tracking_uri="http://130.193.53.137:8000")
if len(client.search_runs(experiment_id, max_results=1)) < 2:
    is_first = True
else:
    is_first = False
    best_run = client.search_runs(experiment_id, order_by=["metrics.roc_auc_mean DESC"], max_results=1)[0]
    print(best_run)
print(is_first)

