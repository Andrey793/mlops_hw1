from pipeline import db
from pyspark.sql import functions as F
import pandas as pd
import os
import shutil
from contextlib import closing

logger = db.logging.getLogger(__name__)

TMP_DATASET = db.CURRENT_DATASET.with_suffix('.tmp')
OLD_DATASET = db.CURRENT_DATASET.with_suffix('.old')


def read_labeled(spark):
    """Lazy DataFrame над labeled_transactions"""
    config = db.get_db_config()
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{config['host']}:{config['port']}/{config['dbname']}")
        .option("dbtable", "labeled_transactions")
        .option("user", config["user"])
        .option("password", config["password"])
        .option("driver", "org.postgresql.Driver")
        .option("preferTimestampNTZ", "true")
        .load()
    )


def remove(path):
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def swap_dataset():
    """Подменяет CURRENT_DATASET на TMP_DATASET
    """
    remove(OLD_DATASET)
    os.replace(db.CURRENT_DATASET, OLD_DATASET)
    os.replace(TMP_DATASET, db.CURRENT_DATASET)
    remove(OLD_DATASET)


def restore_dataset():
    """Если прошлый запуск упал между двумя os.replace в swap_dataset."""
    if not db.CURRENT_DATASET.exists() and OLD_DATASET.exists():
        os.replace(OLD_DATASET, db.CURRENT_DATASET)
        logger.info('Restored dataset from %s', OLD_DATASET)


def load_dataset(spark):
    restore_dataset()
    with closing(db.connect(logger)) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT next_batch_month FROM training_state WHERE id = 1")
            (next_batch_month,) = cur.fetchone()
        month_start = pd.Timestamp(next_batch_month)
        month_end = month_start + pd.DateOffset(months=1)
        new_start = month_start - pd.DateOffset(months=db.TRAINING_WINDOW_MONTHS - 1)

        current = spark.read.parquet(str(db.CURRENT_DATASET))
        already_loaded = not current.filter(F.col('transaction_time') >= month_start).isEmpty()

        if not already_loaded:
            new_data = read_labeled(spark).filter(
                (F.col('transaction_time') >= month_start) & (F.col('transaction_time') < month_end)
            ).select(*db.COLUMNS)
            tail = current.filter(F.col('transaction_time') >= new_start)
            new_dataset = new_data.unionByName(tail)
            new_dataset.write.mode("overwrite").parquet(str(TMP_DATASET))
            swap_dataset()
            logger.info('Added month %s, dataset size %s', month_start.date(),
                        spark.read.parquet(str(db.CURRENT_DATASET)).count())
        else:
            logger.info('%s already in dataset, only updating training_state', month_start.date())

        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                UPDATE training_state SET window_start=%s, window_end=%s, next_batch_month=%s,
                updated_at=now() WHERE id = 1
                """, (new_start.date(), month_start.date(), month_end.date()))
