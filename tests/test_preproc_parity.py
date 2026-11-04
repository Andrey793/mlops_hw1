import numpy as np
import pandas as pd
import pytest

from pipeline.retrain.preproc import run_preproc_spark
from pipeline.spark import spark_session
from shared.preproc import run_preproc


@pytest.fixture(scope="session")
def spark():
    with spark_session("tests") as session:
        yield session


@pytest.fixture
def sample():
    """Граничные случаи: все дни недели, полночь и 23:59, 29 февраля, конец года, NULL в имени."""
    times = pd.to_datetime([
        "2024-01-01 00:00", "2024-01-02 01:15", "2024-01-03 12:00", "2024-01-04 13:30",
        "2024-01-05 18:45", "2024-01-06 22:10", "2024-01-07 23:59",
        "2024-02-29 00:01", "2023-12-31 23:59",
    ])
    n = len(times)
    return pd.DataFrame({
        "transaction_time": times,
        "name_1": ["Ann", None] + ["Bob"] * (n - 2),
        "name_2": ["Lee", "Doe", None] + ["Ray"] * (n - 3),
        "amount": [float(i) for i in range(n)],
    })


def test_spark_preproc_matches_pandas(spark, sample, tmp_path):
    path = tmp_path / "sample.parquet"
    sample.to_parquet(path, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)

    expected = run_preproc(sample)
    got = run_preproc_spark(spark.read.parquet(str(path))).toPandas()[expected.columns]
    got["name"] = got["name"].replace({None: np.nan})

    pd.testing.assert_frame_equal(got, expected)
