from pipeline.db import connect
from pipeline import db
import pandas as pd
import io
from contextlib import closing

def add_batch():
    with closing(connect(db.logger_sup)) as conn:
        if not db.BATCH_PATH.exists():
            raise RuntimeError("Нет батча для добавления")
        batch = pd.read_parquet(db.BATCH_PATH)
        buffer = io.StringIO()
        batch[db.COLUMNS].to_csv(buffer, index=False, header=False, date_format="%Y-%m-%d %H:%M:%S")
        buffer.seek(0)
        with conn:
            with conn.cursor() as cur:
                cur.copy_expert(
                    """
                    COPY labeled_transactions (
                        transaction_time, merch, cat_id, amount, name_1, name_2, gender,
                        street, one_city, us_state, post_code, lat, lon, population_city,
                        jobs, merchant_lat, merchant_lon, target
                    )
                    FROM STDIN WITH (FORMAT CSV)
                    """,
                    buffer,
                )
        db.BATCH_PATH.unlink()
        db.logger_sup.info("В витрину labeled_transactions загружено %s строк.", len(batch))


