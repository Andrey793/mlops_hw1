import os
import logging
import json
import psycopg
import time
from confluent_kafka import Consumer


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/writer/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Set kafka configuration file
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://writer:pass@postgres:5432/scoring")


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS scores (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id TEXT UNIQUE NOT NULL,
    score          DOUBLE PRECISION NOT NULL,
    fraud_flag     BOOLEAN NOT NULL
);
"""

INSERT_SQL = """
INSERT INTO scores (transaction_id, score, fraud_flag)
VALUES (%s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING;
"""


class ProcessingService:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'writer',
            'auto.offset.reset': 'earliest',
            "enable.auto.commit": False
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([SCORING_TOPIC])
        self.connection = self.connect()
        self.init_db()

    def connect(self, retries=10, delay=2):
        for _ in range(retries):
            try:
                return psycopg.connect(DATABASE_URL)
            except psycopg.OperationalError:
                time.sleep(delay)  # Postgres may still be starting
        raise RuntimeError("Could not connect to Postgres")

    def init_db(self):
        with self.connection.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        self.connection.commit()

    def ensure_connection(self):
        if self.connection is None or self.connection.closed:
            logger.warning("Db connection lost, reconnecting...")
            self.connection = self.connect()


    def insert_score(self, msg: dict):
        self.ensure_connection()
        try:
            with self.connection.cursor() as cur:
                cur.execute(INSERT_SQL, (msg["transaction_id"], msg["score"], msg["fraud_flag"]))
            self.connection.commit()
        except psycopg.OperationalError:
            self.connection.close()              
            raise
        except Exception:
            self.connection.rollback()
            raise
        

    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                raw = json.loads(msg.value().decode('utf-8'))[0]
                data = {
                    "transaction_id": str(raw["transaction_id"]),
                    "score": float(raw["score"]),
                    "fraud_flag": bool(raw["fraud_flag"]),
                }
            except (ValueError, KeyError, IndexError, TypeError) as e:
                logger.error(f"Bad message, skipping: {e}")
                self.consumer.commit(message=msg)
                continue

            while True:
                try:
                    self.insert_score(data)
                    break
                except (psycopg.OperationalError, RuntimeError) as e:
                    logger.error(f"DB unavailable, retrying in 2s: {e}")
                    time.sleep(2)
            self.consumer.commit(message=msg)
            logger.info(f"Message processed: {data['transaction_id']}")


if __name__ == "__main__":
    logger.info('Starting Kafka writer service...')
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')