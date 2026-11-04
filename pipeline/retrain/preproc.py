from pipeline import db
from pipeline.spark import spark_session
from pipeline.retrain.load_data import load_dataset
from pyspark.sql import DataFrame, functions as F


def run_preproc_spark(df: DataFrame) -> DataFrame:
    ts = F.col('transaction_time')
    return df.withColumns({
        'name': F.concat(F.col('name_1'), F.lit(' '), F.col('name_2')),
        'hour': F.hour(ts),
        'year': F.year(ts),
        'month': F.month(ts),
        'day_of_month': F.dayofmonth(ts),
        'day_of_week': F.weekday(ts),
    }).drop('transaction_time', 'name_1', 'name_2')


def preproc(spark):
    df = run_preproc_spark(spark.read.parquet(str(db.CURRENT_DATASET)))
    df.write.mode("overwrite").parquet(str(db.CURRENT_DATASET_PROC))
    db.logger_train.info('Preprocessing completed. Output: %s rows, %s columns',
                         spark.read.parquet(str(db.CURRENT_DATASET_PROC)).count(), len(df.columns))


def build_dataset():
    with spark_session("retrain_build_dataset") as spark:
        load_dataset(spark)
        preproc(spark)
