import pytest

from src.app import predict


@pytest.fixture
def transaction():
    numeric_cols = ["terminal_id", "hour_tx_datetime", "tx_amount", "percent_fraud_on_terminal"]
    test_vals = [1, 2.0, 3.0, 0.3]
    return dict(zip(numeric_cols, test_vals))

@pytest.fixture
def bullshit():
    return {"xyz":"lol"}


class TestApp:
    def test_pred(self, transaction):
        res = predict(transaction)
        print(res)
        assert type(res)==dict
        assert type(res["pred"])==int

    def test_pred_bullshit(self, bullshit):
        res = predict(bullshit)
        print(res)
