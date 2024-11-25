import random

import requests
from tqdm import tqdm


def generate_click():
    return {
        "terminal_id": random.randint(0, 1000),
        "hour_tx_datetime": random.randint(0,24),
        "tx_amount": random.uniform(0, 100000),
        "percent_fraud_on_terminal": random.uniform(0, 1),
    }

url = 'http://127.0.0.1:49625/predict'

if __name__ == '__main__':
    for i in tqdm(range(0, 1000)):
        click = generate_click()
        response = requests.post(url, json=click,
                                 headers={'Content-Type': 'application/json'})
        if response.status_code != 200:
            print('Request failed')
            print('Response:', response.text)