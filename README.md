# Real-Time Fraud Detection System

> **Изменение:** раньше сервис принимал только файлы в формате `test.csv` (я думал, что задание требует именно этого). Теперь `fraud_detector` оставляет из входных данных только признаки модели (`model.feature_names_`), поэтому подходит любой CSV с нужными колонками, в том числе `train.csv` с колонкой `target`.

Транзакции из CSV отправляются в Kafka, `fraud_detector` скорит их моделью CatBoost, `writer` сохраняет результат в PostgreSQL, а Streamlit-интерфейс его показывает.

```
interface ──▶ [transactions] ──▶ fraud_detector ──▶ [scoring] ──▶ writer ──▶ PostgreSQL
    ▲                                                                            │
    └──────────────────────────── Check results ◀────────────────────────────────┘
```

| Сервис | Что делает |
|---|---|
| `interface` | Streamlit: загружает CSV, присваивает каждой транзакции `transaction_id` (UUID) и отправляет её отдельным JSON-сообщением в `transactions`. По кнопке **Check results** показывает последние 10 транзакций с `fraud_flag = TRUE` и гистограмму скоров последних 100 |
| `fraud_detector` | препроцессинг (временные признаки), скоринг CatBoost с порогом 0.98, результат отправляет в `scoring` |
| `writer` | пишет результаты из `scoring` в таблицу `scores`, повторные `transaction_id` игнорирует |
| `postgres` | PostgreSQL 16, `postgresql://writer:pass@localhost:5432/scoring` |
| `kafka`, `kafka-setup`, `kafka-ui` | брокер, создание топиков (3 партиции, хранение 7 дней), веб-интерфейс Kafka |

`writer` стартует только после того, как Kafka и Postgres прошли healthcheck, а `kafka-setup` создал топики.

## Запуск

Нужны Docker Engine 20.10+ и Docker Compose v2.

```bash
git clone https://github.com/NikitaMalykhin/mts25_mlops_hw2_real_time_fraud_detection.git
cd mts25_mlops_hw2_real_time_fraud_detection
docker compose up --build
```

- **Streamlit UI:** http://localhost:8501
- **Kafka UI:** http://localhost:8080
- **Логи:** `docker compose logs <service>`

## Использование

1. Загрузите в Streamlit CSV из соревнования [teta-ml-1-2025](https://www.kaggle.com/competitions/teta-ml-1-2025) (`test.csv` или `train.csv`) и нажмите **Отправить**. Для первой проверки хватит ~100 строк.
2. Нажмите **Check results**.
3. Сообщения в топиках `transactions` и `scoring` можно посмотреть в Kafka UI.

Формат сообщения в `scoring` и таблица `scores`:

```json
{"score": 0.995, "fraud_flag": 1, "transaction_id": "d6b0f7a0-8e1a-4a3c-9b2d-5c8f9d1e2f3a"}
```

```sql
CREATE TABLE scores (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id TEXT UNIQUE NOT NULL,
    score          DOUBLE PRECISION NOT NULL,
    fraud_flag     BOOLEAN NOT NULL
);
```
