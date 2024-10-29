"""App"""
import joblib
from fastapi import Depends, FastAPI
from pydantic import BaseModel
from starlette_exporter import PrometheusMiddleware, handle_metrics
from prometheus_client import Counter


class Transaction(BaseModel):
    tranaction_id: int
    tx_datetime: str
    customer_id: int
    terminal_id: int
    tx_amount: float
    tx_time_seconds: int
    tx_time_days: int


# bucket_name = "mlops-hw3-vos"
# s3 = boto3.resource("s3")
# with BytesIO() as data:
#     s3.Bucket(bucket_name).download_fileobj("baseline_model.pkl", data)
#     data.seek(0)
#     model = joblib.load(data)
model = joblib.load("hw_models/model.pkl")

app = FastAPI()
app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", handle_metrics)

ZERO_COUNTER = Counter("zero_answered", "zero_answered")
ALERT_COUNTER = Counter("alert_counter", "alert_counter")


@app.get("/")
def healthcheck():
    return {"status": "OK"}


@app.get("/predict")
def predict(transaction: Transaction = Depends()):
    inf = [
        int(transaction.terminal_id),
        float(transaction.tx_amount),
        int(transaction.tx_time_seconds),
        int(transaction.tx_time_days),
    ]
    # fake event for resetting counter
    if int(transaction.terminal_id) == 13:
        ALERT_COUNTER.reset()
    else:
        ALERT_COUNTER.inc()
    pred = model.predict([inf])
    ans = int(pred)
    if ans == 0:
        ZERO_COUNTER.inc()
    return {"pred": ans}