import os
import sys
import shutil
import pandas as pd
import logging
import json

from confluent_kafka import Consumer, Producer
from shared.preproc import run_preproc

sys.path.append(os.path.abspath('./src'))
from scorer import make_pred, MODEL_PATH

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Set kafka configuration file
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")
BASELINE_MODEL_PATH = os.getenv("BASELINE_MODEL_PATH", "/app/app/baseline/model.cbm")


def ensure_model():
    """Кладёт базовую модель в общий volume, если обученной модели там ещё нет."""
    if not os.path.exists(MODEL_PATH):
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        tmp = MODEL_PATH + ".tmp"
        shutil.copy(BASELINE_MODEL_PATH, tmp)
        os.replace(tmp, MODEL_PATH)
        logger.info("Baseline model copied to %s", MODEL_PATH)


class ProcessingService:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.producer_config = {
             'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
             }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer(self.producer_config)


    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))

                transaction_id = data['transaction_id']
                input_df = pd.DataFrame([data['data']])

                processed_df = run_preproc(input_df)
                submission = make_pred(processed_df, "kafka_stream")

                submission['transaction_id'] = transaction_id

                self.producer.produce(
                    SCORING_TOPIC,
                    value=submission.to_json(orient='records')
                )
                self.producer.flush()
            except Exception as e:
                logger.error(f"Error processing message: {e}")
            self.consumer.commit(message=msg)


if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    ensure_model()
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
