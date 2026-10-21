from pipeline import db
logger = db.logging.getLogger(__name__)
from contextlib import closing

def uniqueness():
    with closing(db.connect(logger)) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM (SELECT DISTINCT * FROM labeled_transactions) t")
            total_unique = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM labeled_transactions")
            total = cur.fetchone()[0]
    logger.info("Uniqueness: %s / %s", total_unique, total)
    return {"check": "uniqueness", "passed": total_unique == total,
            "details": f"{total_unique} уникальных из {total}"}

def completeness():
    # число пропусков
    nulls_sql = ", ".join(f"COUNT(*) - COUNT({col})" for col in db.COLUMNS)
    with closing(db.connect(logger)) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {nulls_sql} FROM labeled_transactions")
            nulls = dict(zip(db.COLUMNS, cur.fetchone()))
    bad = {col: n for col, n in nulls.items() if n > 0}
    logger.info("Completeness: %s", bad or "нет пропусков")
    return {"check": "completeness", "passed": not bad,
            "details": f"NULL по колонкам: {bad}" if bad else "пропусков нет"}

VALIDITY_RULES = {
    "amount <= 0": "amount <= 0",
    "target not in (0, 1)": "target NOT IN (0, 1)",
    "lat/lon вне диапазона": "lat NOT BETWEEN -90 AND 90 OR lon NOT BETWEEN -180 AND 180",
    "merchant_lat/lon вне диапазона": "merchant_lat NOT BETWEEN -90 AND 90 OR merchant_lon NOT BETWEEN -180 AND 180",
    "population_city <= 0": "population_city <= 0",
}

def validity():
    counts_sql = ", ".join(f"COUNT(*) FILTER (WHERE {cond})" for cond in VALIDITY_RULES.values())
    with closing(db.connect(logger)) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {counts_sql} FROM labeled_transactions")
            violations = dict(zip(VALIDITY_RULES, cur.fetchone()))
    bad = {rule: n for rule, n in violations.items() if n > 0}
    logger.info("Validity: %s", bad or "нарушений нет")
    return {"check": "validity", "passed": not bad,
            "details": f"нарушения: {bad}" if bad else "все значения в допустимых диапазонах"}

def continuity():
    with closing(db.connect(logger)) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT m::date
                FROM generate_series(
                    (SELECT date_trunc('month', MIN(transaction_time)) FROM labeled_transactions),
                    (SELECT date_trunc('month', MAX(transaction_time)) FROM labeled_transactions),
                    interval '1 month') m
                WHERE NOT EXISTS (
                    SELECT 1 FROM labeled_transactions
                    WHERE transaction_time >= m AND transaction_time < m + interval '1 month')
                """)
            missing = [str(row[0]) for row in cur.fetchall()]
    logger.info("Continuity: пропущенные месяцы %s", missing)
    return {"check": "continuity", "passed": not missing,
            "details": f"нет данных за месяцы: {missing}" if missing else "пропущенных месяцев нет"}

def retention():
    with closing(db.connect(logger)) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MIN(transaction_time),
                       date_trunc('month', MAX(transaction_time)) - interval '1 month' - %s * interval '1 day'
                FROM labeled_transactions
                """, (db.EXPIRE_LIMIT * 365,))
            min_time, limit = cur.fetchone()
    logger.info("Retention: min %s, граница %s", min_time, limit)
    return {"check": "retention", "passed": min_time >= limit,
            "details": f"самая старая запись {min_time}, допустимо не раньше {limit}"}
