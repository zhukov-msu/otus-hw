"""App"""

import pickle

import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel


# from pyspark.ml.classification import LogisticRegressionModel
# from pyspark.ml.linalg import Vectors
#
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# from pyspark import SparkConf
# from pyspark.sql import SparkSession
# conf = (
#         SparkConf().setAppName('test')
#     )
# spark = SparkSession.builder.config(conf=conf).getOrCreate()

class Transaction(BaseModel):
    terminal_id: int
    hour_tx_datetime: float
    tx_amount: float
    percent_fraud_on_terminal: float


app = FastAPI()
with open("./model.pkl", "rb") as f:
    model = pickle.load(f)
# model = LogisticRegressionModel.load("./lr.model")


@app.get("/")
def healthcheck():
    return {"status": "OK"}


@app.post("/predict")
def predict(transaction: Transaction):
    numeric_cols = ["terminal_id", "hour_tx_datetime", "tx_amount", "percent_fraud_on_terminal"]
    try:
        # vector = Vectors.dense([transaction[col] for col in numeric_cols])
        vector = np.array([
            int(transaction.terminal_id),
            float(transaction.hour_tx_datetime),
            float(transaction.tx_amount),
            float(transaction.percent_fraud_on_terminal)
        ]).reshape(1, -1)
        pred = model.predict(vector)
        ans = int(pred)
        return {"pred": ans}
    except Exception as e:
        return {"error": str(e)}

