import pandas as pd
import logging
from catboost import CatBoostClassifier

# Настройка логгера
logger = logging.getLogger(__name__)

logger.info('Importing pretrained model...')

# Import model
model = CatBoostClassifier()
model.load_model('./models/model.cbm')

# Define optimal threshold
model_th = 0.98
logger.info('Pretrained model imported successfully...')


def make_pred(dt, source_info="kafka"):

    print(dt.dtypes)

    # Меняем формат категориальных фичей на string перед скорингом
    cat_cols = ['gender', 'merch', 'cat_id', 'one_city', 'us_state', 'jobs', 'street', 'name']
    for col in cat_cols:
        if col in dt.columns:
            dt[col] = dt[col].astype(str)

    # Calculate score
    submission = pd.DataFrame({
        'score':  model.predict_proba(dt)[:, 1],
        'fraud_flag': (model.predict_proba(dt)[:, 1] > model_th) * 1
    })
    logger.info(f'Prediction complete for data from {source_info}')

    # Return proba for positive class
    return submission