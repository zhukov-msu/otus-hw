"""App"""

from fastapi import FastAPI
from pydantic import BaseModel
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.linalg import Vectors


class Transaction(BaseModel):
    tranaction_id: int
    tx_datetime: str
    customer_id: int
    terminal_id: int
    tx_amount: float
    tx_time_seconds: int
    tx_time_days: int


app = FastAPI()
model = LogisticRegressionModel.load("lr.model")
# app.add_route("/predict", predict)


@app.get("/")
def healthcheck():
    return {"status": "OK"}


@app.get("/predict")
def predict(transaction: dict):
    numeric_cols = ["terminal_id", "hour_tx_datetime", "tx_amount", "percent_fraud_on_terminal"]
    vector = Vectors.dense([transaction[col] for col in numeric_cols])
    pred = model.predict(vector)
    ans = int(pred)
    return {"pred": ans}
