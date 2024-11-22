import json
import random
from typing import Dict, NamedTuple

import kafka
from faker import Faker

fake = Faker()

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecordMetadata(NamedTuple):
    key: int
    topic: str
    partition: int
    offset: int


def check_kafka_connection(consumer):
    try:
        partitions = consumer.partitions_for_topic('input')
        print(partitions)
        if partitions:
            logger.info("Successfully connected to Kafka topic 'input' with partitions: %s", partitions)
        else:
            logger.warning("Connected to Kafka but no partitions found for topic 'input'.")
    except Exception as e:
        logger.error("Failed to connect to Kafka topic 'input': %s", e)

def main():
    kafka_servers = ["rc1b-3kiaqqqtp7bu7kue.mdb.yandexcloud.net:9091"]

    producer = kafka.KafkaProducer(
        bootstrap_servers=kafka_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username="mlops",
        sasl_plain_password="otus-mlops",
        ssl_cafile="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
        api_version=(0, 11, 5),
        value_serializer=serialize,
        # max_block_ms=1200000,
        # request_timeout_ms=120000,
    )


    # consumer = kafka.KafkaConsumer(
    #     'input',
    #     bootstrap_servers=kafka_servers,
    #     security_protocol="SASL_SSL",
    #     sasl_mechanism="SCRAM-SHA-512",
    #     sasl_plain_username='mlops',
    #     sasl_plain_password='otus-mlops',
    #     ssl_cafile="/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
    #     fetch_min_bytes=100000,
    #     fetch_max_wait_ms=3000,
    #     # session_timeout_ms=30000,
    #     api_version=(0, 11, 5),
    #     group_id='consume-to-predict',
    #     auto_offset_reset='earliest',  # Читает с самого начала, если нет смещения
    #     enable_auto_commit=True
    # )

    # Проверка подключения к Kafka
    # check_kafka_connection(consumer)

    # consumer = kafka.KafkaConsumer(group_id='test', bootstrap_servers=[kafka_server])
    # topics = consumer.topics()

    # if not topics:
    #     raise RuntimeError()

    try:
        while True:
            record_md = send_message(producer, "input")
            print(
                f"Msg sent. Key: {record_md.key}, topic: {record_md.topic}, partition:{record_md.partition}, offset:{record_md.offset}"
            )
            # time.sleep(10)
    except KeyboardInterrupt:
        print(" KeyboardInterrupted!")
        producer.flush()
        producer.close()


def send_message(producer: kafka.KafkaProducer, topic: str) -> RecordMetadata:
    click = generate_click()
    key = str(click["tranaction_id"]).encode("ascii")
    print(key)
    print(click)
    future = producer.send(
        topic=topic,
        key=key,
        value=click,
    )
    print(1)

    # Block for 'synchronous' sends
    record_metadata = future.get(timeout=1)
    return RecordMetadata(
        key=click["tranaction_id"],
        topic=record_metadata.topic,
        partition=record_metadata.partition,
        offset=record_metadata.offset,
    )


def generate_click() -> Dict:
    return {
        "tranaction_id": random.randint(0, 1000),
        "tx_datetime": fake.date_time_between(start_date="-1y", end_date="+1y").isoformat(),
        "customer_id": random.randint(0, 10000),
        "terminal_id": random.randint(0, 1000),
        "tx_amount": random.uniform(0, 100000),
        "tx_time_seconds": random.randint(0, 1000000),
        "tx_time_days": random.randint(0, 100),
    }


def serialize(msg: Dict) -> bytes:
    return json.dumps(msg).encode("utf-8")


if __name__ == "__main__":
    main()