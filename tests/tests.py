import pytest

from src.app import predict


@pytest.fixture
def transaction():
    numeric_cols = ["terminal_id", "hour_tx_datetime", "tx_amount", "percent_fraud_on_terminal"]
    return {col:i for i, col in enumerate(numeric_cols)}


class TestApp:
    def test_pred(self, transaction):
        res = predict(transaction)
        assert type(res)==dict
        assert type(res["pred"])==int