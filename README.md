# Real-Time Fraud Detection + Airflow retraining

`transaction_simulator` шлёт транзакции в Kafka, `fraud_detector` скорит их CatBoost-моделью, `writer` пишет результат в Postgres (таблица `scores`). Airflow каждые 5 минут пополняет размеченные данные и переобучает модель. Новая версия попадает в прод, только если она лучше текущей.

## Запуск

Нужны Docker Compose v2 и `train.csv` из [teta-ml-1-2025](https://www.kaggle.com/competitions/teta-ml-1-2025).

```bash
cp .env.example .env                  # укажите TRAIN_DATA_CSV=/abs/path/to/train.csv и JWT_SECRET
echo "AIRFLOW_UID=$(id -u)" >> .env   # Linux/WSL: права на ./logs
docker compose up --build -d
docker compose ps                     # data_loader и airflow-init — Exited (0), остальные — running
```

- **Airflow UI:** http://localhost:8081, логин и пароль `airflow` / `airflow`
- **Kafka UI:** http://localhost:8080

## DAG'и

В Airflow UI включите оба DAG'а: они создаются на паузе.

- **`update_dataset`** запускается каждые 5 минут, или сразу по кнопке **Trigger**. Он берёт выборку из витрины, сдвигает её даты на следующий месяц, удаляет данные старше 3 лет и дописывает батч в `labeled_transactions`.
- **`retrain_catboost`** стартует сам после каждого `update_dataset`. Задачи: `load_data` → `preproc` → `train` → `publish`.

## Проверка, что скорит опубликованная модель

```bash
# 1. транзакции скорятся: count растёт
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM scores;"'

# 2. результат последнего обучения (то же видно в логе задачи publish в Airflow UI)
docker compose exec fraud_detector cat /app/models/test_auc.json
#    в логе publish: "... Model saved to /models/model.cbm" или "... Model not saved"

# 3. fraud_detector скорит опубликованной версией: version в логе = version в model_meta.json
docker compose logs fraud_detector | grep "Loaded model" | tail -1
docker compose exec fraud_detector cat /app/models/model_meta.json
```

`publish` при каждой публикации пишет `model_meta.json` с версией и AUC. `fraud_detector` перед скорингом проверяет, менялись ли `model.cbm` и `model_meta.json`, и перечитывает модель. Каждая публикация даёт новую строку `Loaded model ... (version ...)`. Если модель отклонена, новой строки не будет. `version baseline` — стартовая модель, которую ещё ничего не заменило.

## Когда модель публикуется

Окно обучения — 4 последних месяца (`TRAINING_WINDOW_MONTHS`), на каждом запуске сдвигается на месяц вперёд. Делится **по времени**:

| месяцы M-3, M-2 | месяц M-1 | месяц M (новый) |
|---|---|---|
| train | validation (early stopping) | test |

`train` считает **ROC AUC на месяце M** и для кандидата, и для текущей прод-модели. `publish` атомарно (`os.replace`) подменяет `model.cbm`, только если `auc_new > auc_old`.

Исключение — стартовая модель (`fraud_detector/app/baseline/model.cbm`). Она обучена на всём `train.csv`, включая тестовые месяцы, поэтому её AUC завышен утечкой (≈0.999) и с кандидатом несравним. Пока в проде стоит она (нет `model_meta.json`), первый кандидат публикуется без сравнения. Дальше модели сравниваются только с теми, что опубликовал пайплайн.

Почему так:
- **Одна выборка для обеих моделей.** AUC на разных месяцах сравнивать нельзя.
- **Месяц M не видела ни одна из моделей.** Прод обучалась до его появления, у кандидата он отложен.
- **Тест — будущее относительно train**, как и в проде.
- **Early stopping идёт на M-1, а не на тесте**, иначе AUC кандидата завышен.
- **ROC AUC не зависит от порога** (0.98) и устойчив к дисбалансу классов.

Что можно улучшить:
- добавить PR AUC и recall при пороге 0.98;
- требовать прирост больше бутстреп-шума, а не просто «> 0»;
- писать версию модели в `scores`.

## Остановка

`docker compose down`, с удалением данных: `docker compose down -v`. Удаление данных нужно и после смены логина/пароля Postgres в `.env`.
