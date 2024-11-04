import pytest

from src.app import predict, Transaction


@pytest.fixture
def transaction():
    return Transaction(terminal_id=1, hour_tx_datetime=2.0, tx_amount=3.0, percent_fraud_on_terminal=0.3)


# @pytest.fixture
# def bullshit():
#     return Transaction(terminal_id="ssssss", hour_tx_datetime=2.0, tx_amount=3.0, percent_fraud_on_terminal=0.3)


def test_pred(transaction):
    res = predict(transaction)
    print(res)
    assert type(res)==dict
    assert type(res["pred"])==int

# def test_pred_bullshit(bullshit):
#     res = predict(bullshit)
#     print(res)
