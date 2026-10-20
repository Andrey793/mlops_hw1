import logging
import os
import time
from pathlib import Path

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger_sup = logging.getLogger("supply pipeline")
logger_train = logging.getLogger("retrain pipeline")

TRAINING_DIR = Path(os.getenv("TRAINING_DATA_DIR", "/training_data"))
CURRENT_DATASET = TRAINING_DIR / "current.parquet"
CURRENT_DATASET_PROC = TRAINING_DIR / "current_proc.parquet"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 50000))
EXPIRE_LIMIT = int(os.getenv("EXPIRE_LIMIT", 3)) #years
# окно обучения в месяцах: последний — test, предпоследний — validation, остальные — train
TRAINING_WINDOW_MONTHS = int(os.getenv("TRAINING_WINDOW_MONTHS", 4))
BATCH_DIR = Path(os.getenv("BATCH_DATA_DIR", "/batch_data"))
BATCH_PATH = BATCH_DIR / "batch.parquet"
MODEL_DIR = Path(os.getenv("MODEL_DIR", "/models"))
TEST_AUC_PATH = MODEL_DIR / "test_auc.json"
MODEL_PATH = MODEL_DIR / "model.cbm"
# есть только у моделей, опубликованных пайплайном
MODEL_META_PATH = MODEL_DIR / "model_meta.json"


COLUMNS = [
    "transaction_time",
    "merch",
    "cat_id",
    "amount",
    "name_1",
    "name_2",
    "gender",
    "street",
    "one_city",
    "us_state",
    "post_code",
    "lat",
    "lon",
    "population_city",
    "jobs",
    "merchant_lat",
    "merchant_lon",
    "target",
]


def get_db_config():
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def connect(logger):
    config = get_db_config()
    last_error = None
    for attempt in range(1, 31):
        try:
            return psycopg2.connect(**config)
        except psycopg2.OperationalError as exc:
            last_error = exc
            logger.info("Postgres ещё не готов (%s/30): %s", attempt, exc)
            time.sleep(2)
    raise last_error

def row_count(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM labeled_transactions")
        return cur.fetchone()[0]