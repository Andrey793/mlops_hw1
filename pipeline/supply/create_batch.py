from pipeline.db import connect
from pipeline import db
import pandas as pd
import calendar
from contextlib import closing


def next_month_start(max_time: pd.Timestamp) -> pd.Timestamp:
    """Возвращает первое число месяца, следующего за месяцем max_time."""
    return (pd.Timestamp(max_time).to_period("M") + 1).to_timestamp()

def shift_into_month(frame: pd.DataFrame, month_start: pd.Timestamp) -> pd.DataFrame:
    """Переписывает transaction_time в заданный месяц, сохраняя день, час и минуту.

    Если в целевом месяце нет такого дня, день обрезается до последнего числа.
    Секунды обнуляются. Исходный frame не меняется.
    """
    shifted = frame.copy()
    times = pd.to_datetime(shifted["transaction_time"])
    year = int(month_start.year)
    month = int(month_start.month)
    last_day = calendar.monthrange(year, month)[1]
    shifted["transaction_time"] = pd.to_datetime(
        {
            "year": year,
            "month": month,
            "day": times.dt.day.clip(upper=last_day),
            "hour": times.dt.hour,
            "minute": times.dt.minute,
        }
    )
    return shifted

def max_date(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(transaction_time) FROM labeled_transactions")
        result = cur.fetchone()[0]
    if result is None:
        raise RuntimeError("Витрина labeled_transactions пуста")
    return result


def create_batch():
    with closing(connect(db.logger_sup)) as conn:
        total = db.row_count(conn)
        if total == 0:
            raise RuntimeError("Витрина labeled_transactions пуста")
        sample_size = min(db.BATCH_SIZE, total)
        pct = min(100.0, 100.0 * sample_size / total * 1.2)
        batch = pd.read_sql(
                    """
                    SELECT *
                    FROM labeled_transactions
                    TABLESAMPLE BERNOULLI (%s)
                    ORDER BY random()
                    LIMIT %s
                    """,
                    conn,
                    params=(pct, sample_size,),
                )
        batch = shift_into_month(batch, next_month_start(max_date(conn)))
        batch.to_parquet(db.BATCH_PATH, index=False)
        db.logger_sup.info("Создан батч из %s строк.", len(batch))