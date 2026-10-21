import pandas as pd
import logging
from catboost import CatBoostClassifier
import os
import json

# Настройка логгера
logger = logging.getLogger(__name__)

MODEL_PATH = os.getenv("MODEL_PATH", "/app/models/model.cbm")
MODEL_META_PATH = os.path.join(os.path.dirname(MODEL_PATH), "model_meta.json")
_model, _mtime, _version = None, 0.0, 1.0


def model_version():
    """Версия из model_meta.json, который пишет publish"""
    try:
        with open(MODEL_META_PATH) as f:
            return json.load(f)["version"]
    except (OSError, ValueError, KeyError):
        return "baseline"


def get_model():
    """Загружает модель и перечитывает её, если publish заменил файл."""
    global _model, _mtime, _version
    # publish меняет сначала модель, потом meta
    mtime = os.path.getmtime(MODEL_PATH)
    if os.path.exists(MODEL_META_PATH):
        mtime = max(mtime, os.path.getmtime(MODEL_META_PATH))
    if _model is None or mtime > _mtime:
        m = CatBoostClassifier()
        m.load_model(MODEL_PATH)
        _model, _mtime, _version = m, mtime, model_version()
        logger.info("Loaded model %s (version %s, mtime %s)", MODEL_PATH, model_version(), mtime)
    return _model

# Define optimal threshold
model_th = 0.98


def make_pred(dt, source_info="kafka"):
    model = get_model()
    dt = dt[model.feature_names_].copy()
    cat_cols = ['gender', 'merch', 'cat_id', 'one_city', 'us_state', 'jobs', 'street', 'name']
    for col in cat_cols:
        if col in dt.columns:
            dt[col] = dt[col].astype(str)

    proba = model.predict_proba(dt)[:, 1]
    submission = pd.DataFrame({
        'score':  proba,
        'fraud_flag': (proba > model_th) * 1,
        'model_version': _version
    })
    logger.info(f'Prediction complete for data from {source_info}')

    return submission
