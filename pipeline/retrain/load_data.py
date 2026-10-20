from pipeline import db
import pandas as pd
import os
from contextlib import closing

logger = db.logging.getLogger(__name__)

def load_dataset():
    with closing(db.connect(logger)) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT next_batch_month FROM training_state WHERE id = 1")
            (next_batch_month,) = cur.fetchone()
        month_start = pd.Timestamp(next_batch_month)
        month_end = month_start + pd.DateOffset(months=1)
        # окно — TRAINING_WINDOW_MONTHS месяцев, заканчивая новым месяцем
        new_start = month_start - pd.DateOffset(months=db.TRAINING_WINDOW_MONTHS - 1)

        df = pd.read_parquet(db.CURRENT_DATASET)
        # previous run replaced the file but crashed before commit
        already_loaded = (df['transaction_time'] >= month_start).any()

        if not already_loaded:
            new_df = pd.read_sql(
                    f"""
                    SELECT {', '.join(db.COLUMNS)}
                    FROM labeled_transactions
                    WHERE transaction_time >= %s AND transaction_time < %s
                    ORDER BY transaction_time
                    """,
                    conn,
                    params=(month_start.to_pydatetime(), month_end.to_pydatetime()),
                    )
            df = pd.concat([df[df['transaction_time'] >= new_start], new_df], ignore_index=True)
            tmp = db.CURRENT_DATASET.with_suffix('.tmp')
            df.to_parquet(tmp, index=False)
            os.replace(tmp, db.CURRENT_DATASET)
            logger.info('Added %s rows for %s, dataset size %s', len(new_df), month_start.date(), len(df))
        else:
            logger.info('%s already in dataset, only updating training_state', month_start.date())

        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                UPDATE training_state SET window_start=%s, window_end=%s, next_batch_month=%s,
                updated_at=now() WHERE id = 1
                """, (new_start.date(), month_start.date(), month_end.date()))
