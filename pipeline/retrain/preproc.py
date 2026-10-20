from pipeline import db
import pandas as pd
from shared.preproc import run_preproc


def preproc():
    df = pd.read_parquet(db.CURRENT_DATASET)
    df = run_preproc(df)
    df.to_parquet(db.CURRENT_DATASET_PROC, index=False)
    db.logger_train.info('Preprocessing completed. Output shape: %s', df.shape)

