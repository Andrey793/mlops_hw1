import os
from contextlib import contextmanager

from pyspark.sql import SparkSession


@contextmanager
def spark_session(app_name):
    """Локальная Spark-сессия, которая гарантированно останавливается после with."""
    spark = (
        SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.jars", os.environ["SPARK_JARS"])
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
    )
    try:
        yield spark
    finally:
        spark.stop()

    