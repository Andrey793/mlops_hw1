from pipeline import db
import os
import json
from datetime import datetime, timezone

def publish():
    with open(db.TEST_AUC_PATH, 'r') as f:
        auc_dict = json.load(f)
    if auc_dict['auc'] < auc_dict['auc_tmp']:
        os.replace(db.MODEL_DIR / 'model_tmp.cbm', db.MODEL_PATH)
        meta = {
            'version': datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'),
            'test_auc': auc_dict['auc_tmp'],
            'mtime': os.path.getmtime(db.MODEL_PATH),
        }
        tmp = db.MODEL_META_PATH.with_suffix('.tmp')
        tmp.write_text(json.dumps(meta))
        os.replace(tmp, db.MODEL_META_PATH)
        db.logger_train.info('Old auc: %s, new auc: %s. Model %s saved to %s',
                             auc_dict['auc'], auc_dict['auc_tmp'], meta['version'], db.MODEL_PATH)
    else:
        db.logger_train.info('Old auc: %s, new auc: %s. Model not saved', auc_dict['auc'], auc_dict['auc_tmp'])
