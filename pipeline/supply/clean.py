from pipeline.db import connect
from pipeline import db
import datetime
from pipeline.supply.create_batch import max_date
from contextlib import closing

def filter_db():
    with closing(connect(db.logger_sup)) as conn:
        date_limit = max_date(conn) - datetime.timedelta(days=db.EXPIRE_LIMIT * 365)
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM labeled_transactions WHERE transaction_time < %s", (date_limit,))
                db.logger_sup.info("В витрине labeled_transactions удалено %s строк.", cur.rowcount)