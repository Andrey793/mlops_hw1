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
- **Data Quality отчёт:** http://localhost:8090

## DAG'и

В Airflow UI включите все три DAG'а: они создаются на паузе.

- **`update_dataset`** запускается каждые 5 минут, или сразу по кнопке **Trigger**. Он берёт выборку из витрины, сдвигает её даты на следующий месяц, удаляет данные старше 3 лет и дописывает батч в `labeled_transactions`.
- **`retrain_catboost`** стартует сам после каждого `update_dataset`. Задачи: `load_data` → `preproc` → `train` → `publish`.
- **`data_quality`** тоже стартует после каждого `update_dataset` и проверяет таблицу `labeled_transactions`. Каждое правило — отдельная задача, они идут параллельно, а `report` собирает результаты в HTML-отчёт.

## Data Quality

| Правило | Задача | Что проверяется |
|---|---|---|
| Уникальность | `uniqueness` | в таблице нет полностью одинаковых строк (например, после повторного `add_batch`) |
| Полнота | `completeness` | ни в одной колонке нет `NULL` |
| Валидность | `validity` | `amount > 0`, `target ∈ {0, 1}`, координаты клиента и мерчанта в допустимых диапазонах, `population_city > 0` |
| Непрерывность | `continuity` | между первым и последним месяцем нет месяцев без данных |
| Срок хранения | `retention` | нет записей старше `EXPIRE_LIMIT` лет (проверяет работу `filter_db`) |

Правила лежат в `pipeline/quality/checks.py`. Каждая функция возвращает `{"check", "passed", "details"}`, задача `report` (`pipeline/quality/report.py`) собирает их в `index.html` и атомарно (`os.replace`) подменяет файл в `./report`. Сервис `report` (nginx) отдаёт эту папку на http://localhost:8090. При старте контейнера отчёт сбрасывается в заглушку, а страница сама обновляется каждые 30 секунд.

## Проверка, что скорит опубликованная модель

```bash
# 1. результат последнего обучения (то же видно в логе задачи publish в Airflow UI)
docker compose exec fraud_detector cat /app/models/test_auc.json

# 2. fraud_detector скорит опубликованной версией: version в логе = version в model_meta.json
docker compose logs fraud_detector | grep "Loaded model" | tail -1
docker compose exec fraud_detector cat /app/models/model_meta.json
```

`publish` при каждой публикации пишет `model_meta.json` с версией и AUC. `fraud_detector` перед скорингом проверяет, менялись ли `model.cbm` и `model_meta.json`, и перечитывает модель. Версия модели для каждого скора передается в Kafka.

## Когда модель публикуется

Окно обучения — 4 последних месяца (`TRAINING_WINDOW_MONTHS`), на каждом запуске сдвигается на месяц вперёд. Делится **по времени**:

| месяцы M-3, M-2 | месяц M-1 | месяц M (новый) |
|---|---|---|
| train | validation (early stopping) | test |

`train` считает **ROC AUC на месяце M** и для кандидата, и для текущей прод-модели. `publish` атомарно (`os.replace`) подменяет `model.cbm`, только если `auc_new > auc_old`.

Исключение — стартовая модель (`fraud_detector/app/baseline/model.cbm`). Она обучена на всём `train.csv`, включая тестовые месяцы, поэтому её AUC завышен утечкой (≈0.999) и с кандидатом несравним. Пока в проде стоит она (нет `model_meta.json`), первый кандидат публикуется без сравнения. Дальше модели сравниваются только с теми, что опубликовал пайплайн.

## Остановка

`docker compose down`, с удалением данных: `docker compose down -v`. Удаление данных нужно и после смены логина/пароля Postgres в `.env`.
