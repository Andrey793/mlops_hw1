import catboost
import mlflow
import pandas as pd
from pipeline import db
import json
from contextlib import closing
from sklearn.metrics import roc_auc_score

CAT_FEATURES = ["gender", "merch", "cat_id", "one_city", "us_state", "jobs", "street", "name"]
EXPERIMENT = "fraud_retrain"
PARAMS = dict(
    iterations=100,
    learning_rate=0.1,
    depth=6,
    l2_leaf_reg=3,
    random_seed=42,
    eval_metric='AUC',
    early_stopping_rounds=10,
)


def training_window():
    with closing(db.connect(db.logger_train)) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT window_start, window_end FROM training_state WHERE id = 1")
            window_start, window_end = cur.fetchone()
    return {'window_start': str(window_start), 'window_end': str(window_end)}


def train():
    df = pd.read_parquet(db.CURRENT_DATASET_PROC)
    period = df["year"] * 12 + df["month"]
    test_mask = period == period.max()
    val_mask = period == period[~test_mask].max()
    train_mask = ~(test_mask | val_mask)
    X, y = df.drop(columns=["target"]), df["target"]
    X_train, y_train = X[train_mask], y[train_mask]
    X_val, y_val = X[val_mask], y[val_mask]
    X_test, y_test = X[test_mask], y[test_mask]
    db.logger_train.info('Split by month: train %s rows (%s months), val %s, test %s',
                         len(X_train), period[train_mask].nunique(), len(X_val), len(X_test))
    if len(X_train) == 0:
        raise ValueError("Training window is too short: no months left for train")
    if y_test.nunique() < 2:
        raise ValueError("Test month contains only one class, AUC is undefined")

    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run() as run:
        mlflow.log_params({**PARAMS, **training_window(),
                           'train_rows': len(X_train), 'val_rows': len(X_val), 'test_rows': len(X_test)})

        model = catboost.CatBoostClassifier(**PARAMS)
        model.fit(X_train, y_train, eval_set=(X_val, y_val), use_best_model=True,
                  cat_features=CAT_FEATURES, verbose=False)
        new_auc = roc_auc_score(y_test, model.predict_proba(X_test[model.feature_names_])[:, 1])

        old_auc = -1.0
        if not db.MODEL_META_PATH.exists():
            db.logger_train.info('Prod model is baseline, its AUC is not comparable; candidate will be published')
        elif db.MODEL_PATH.exists():
            prod = catboost.CatBoostClassifier()
            prod.load_model(str(db.MODEL_PATH))
            old_auc = roc_auc_score(y_test, prod.predict_proba(X_test[prod.feature_names_])[:, 1])

        mlflow.log_metrics({'auc_new': new_auc, 'auc_old': old_auc})
        mlflow.catboost.log_model(model, name='model')

    model.save_model(str(db.MODEL_DIR / 'model_tmp.cbm'))
    db.TEST_AUC_PATH.write_text(json.dumps({'auc': old_auc, 'auc_tmp': new_auc, 'run_id': run.info.run_id}))
    db.logger_train.info('Model saved to %s, test AUC new=%.4f old=%.4f, mlflow run %s',
                         db.MODEL_DIR / 'model_tmp.cbm', new_auc, old_auc, run.info.run_id)
