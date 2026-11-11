Репозиторий состоит из 3х веток.

Main: первая простая версия только с Kafka и интерфейсом.

Airflow: Вторая версия с добавлением DAG для переобучения и обновления датасета.

Monitoring: третья версия с метриками Prometheus и дашбордами Grafana.

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
- **Grafana:** http://localhost:3000, логин и пароль `admin` / `admin`
- **Prometheus:** http://localhost:9090 (статус сборщиков — http://localhost:9090/targets)


## Мониторинг

Prometheus раз в 2 секунды собирает метрики с `fraud_detector:8080`, `writer:8081` и `node-exporter:9100`. Дашборды лежат в `grafana/dashboards/` и подгружаются в Grafana автоматически:

| Дашборд | Что показывает |
|---|---|
| **Fraud Detector** | скорость потока, время обработки, доля фрода, распределение скоров во времени, версия модели, ошибки обработки |
| **Writer** | скорость и время записи в БД, число выявленного фрода, распределение записанных скоров |
| **Infrastructure** | CPU, память, сеть, диск, статус сборщиков (`up`) |

**Метрики растут вместе с потоком.** На `/targets` все цели `UP`. В Prometheus запросите `total_transactions_total` и `scores_total`: значения растут с каждым обновлением. На дашбордах «Transactions / s» и «Writes / s» держатся около 10/с (`SEND_INTERVAL_SECONDS=0.1`). Если остановить `transaction_simulator`, графики падают до нуля.

**Отметка новой версии модели.** При каждой загрузке нового файла модели (без перезапуска контейнера) `fraud_detector` увеличивает счётчик `model_loads_total{version="..."}`, а `model_info{version}` показывает текущую версию. Чтобы проверить: нажмите Trigger на `update_dataset`, дождитесь `publish` в `retrain_catboost` (модель публикуется, только если AUC вырос). Через несколько секунд на дашбордах Fraud Detector и Writer появится фиолетовая вертикальная отметка «Model loaded», панель «Current model version» покажет новую версию, а на «Model version timeline» начнётся новая строка.

**Дополнительная метрика — `processing_errors_total{error_type}`.** Счётчик сообщений, которые `fraud_detector` не смог обработать. Метка — класс исключения (`KeyError`, `ValueError`, …). Такие сообщения коммитятся в Kafka и до `scoring` не доходят, то есть это потерянные транзакции. Без метрики их видно только в логах. Панель «Processing errors / s» на дашборде Fraud Detector: при нормальной работе показывает 0.

## Остановка

`docker compose down`, с удалением данных: `docker compose down -v`. Удаление данных нужно и после смены логина/пароля Postgres в `.env`.
