from pipeline import db
import os
import json
import mlflow
from mlflow import MlflowClient

REGISTERED_MODEL = "fraud_detector"
ALIAS = "champion"


def register(run_id):
    client = MlflowClient()
    existing = client.search_model_versions(f"name='{REGISTERED_MODEL}' and run_id='{run_id}'")
    mv = existing[0] if existing else mlflow.register_model(f"runs:/{run_id}/model", REGISTERED_MODEL)
    client.set_registered_model_alias(REGISTERED_MODEL, ALIAS, mv.version)
    return mv.version


def publish():
    with open(db.TEST_AUC_PATH, 'r') as f:
        auc_dict = json.load(f)
    if auc_dict['auc'] < auc_dict['auc_tmp']:
        # registry — источник правды, файл — доставка модели в fraud_detector
        version = register(auc_dict['run_id'])
        os.replace(db.MODEL_DIR / 'model_tmp.cbm', db.MODEL_PATH)
        meta = {
            'version': f'v{version}',
            'run_id': auc_dict['run_id'],
            'test_auc': auc_dict['auc_tmp'],
            'mtime': os.path.getmtime(db.MODEL_PATH),
        }
        tmp = db.MODEL_META_PATH.with_suffix('.tmp')
        tmp.write_text(json.dumps(meta))
        os.replace(tmp, db.MODEL_META_PATH)
        db.logger_train.info('Old auc: %s, new auc: %s. Model %s (%s@%s) saved to %s',
                             auc_dict['auc'], auc_dict['auc_tmp'], meta['version'],
                             REGISTERED_MODEL, ALIAS, db.MODEL_PATH)
    else:
        db.logger_train.info('Old auc: %s, new auc: %s. Model not saved', auc_dict['auc'], auc_dict['auc_tmp'])
