"""App"""

import pickle

import numpy as np
from fastapi import FastAPI
from prometheus_client import Counter
from pydantic import BaseModel
from starlette_exporter import PrometheusMiddleware, handle_metrics


class Transaction(BaseModel):
    terminal_id: int
    hour_tx_datetime: float
    tx_amount: float
    percent_fraud_on_terminal: float


app = FastAPI()
app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", handle_metrics)

ZERO_COUNTER = Counter("zero_answered", "zero_answered")
ALERT_COUNTER = Counter("alert_counter", "alert_counter")

with open("./model.pkl", "rb") as f:
    model = pickle.load(f)

@app.get("/")
def healthcheck():
    return {"status": "OK"}


@app.post("/predict")
def predict(transaction: Transaction):
    try:
        vector = np.array([
            int(transaction.terminal_id),
            float(transaction.hour_tx_datetime),
            float(transaction.tx_amount),
            float(transaction.percent_fraud_on_terminal)
        ]).reshape(1, -1)
        if int(transaction.terminal_id) == 13:
            ALERT_COUNTER.reset()
        else:
            ALERT_COUNTER.inc()
        pred = model.predict(vector)
        ans = int(pred)
        if ans == 0:
            ZERO_COUNTER.inc()
        return {"pred": ans}
    except Exception as e:
        return {"error": str(e)}

