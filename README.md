# Real-Time Fraud Detection System

Система потокового скоринга транзакций на мошенничество: транзакции поступают в Kafka, ML-сервис их скорит, результаты сохраняются в PostgreSQL и отображаются в веб-интерфейсе.

## 🏗️ Архитектура

```
interface ──▶ [transactions] ──▶ fraud_detector ──▶ [scoring] ──▶ writer ──▶ PostgreSQL
    ▲                                                                            │
    └──────────────────────────── Check results ◀────────────────────────────────┘
```

Компоненты системы:
1. **`interface`** (Streamlit UI):

   Создан для удобной симуляции потоковых данных с транзакциями. Реальный продукт использовал бы прямой поток данных из других систем.
    - Имитирует отправку транзакций в Kafka через CSV-файлы.
    - Генерирует уникальные ID для транзакций.
    - Загружает транзакции отдельными сообщениями формата JSON в топик kafka `transactions`.
    - По кнопке **Check results** читает результаты из PostgreSQL:
      - выводит таблицу последних 10 транзакций с `fraud_flag = TRUE`;
      - строит гистограмму скоров последних 100 транзакций.

2. **`fraud_detector`** (ML Service):
   - Загружает предобученную модель CatBoost (`models/model.cbm`).
   - Выполняет препроцессинг данных:
     - Извлечение временных признаков
   - Производит скоринг с порогом 0.98.
   - Выгружает результат скоринга в топик kafka `scoring`.

3. **`writer`** (DB Writer):
   - Читает результаты скоринга из топика `scoring`.
   - Записывает их в таблицу `scores` базы PostgreSQL (повторные `transaction_id` игнорируются).

4. **`postgres`** (PostgreSQL 16):
   - База `scoring`, пользователь `writer` / пароль `pass`, порт 5432.

5. **Kafka Infrastructure**:
   - Zookeeper + Kafka брокер
   - `kafka-setup`: автоматически создает топики `transactions` и `scoring`
   - Kafka UI: веб-интерфейс для мониторинга сообщений (порт 8080)

Порядок запуска контролируется через `depends_on` с условиями: `writer` стартует только после того, как Kafka и PostgreSQL прошли healthcheck, а `kafka-setup` успешно создал топики.

## 🚀 Быстрый старт

### Требования
- Docker Engine 20.10+
- Docker Compose v2 (команда `docker compose`)

### Запуск
```bash
git clone https://github.com/NikitaMalykhin/mts25_mlops_hw2_real_time_fraud_detection.git
cd mts25_mlops_hw2_real_time_fraud_detection

# Сборка и запуск всех сервисов
docker compose up --build
```
После запуска:
- **Streamlit UI**: http://localhost:8501
- **Kafka UI**: http://localhost:8080
- **PostgreSQL**: `postgresql://writer:pass@localhost:5432/scoring`
- **Логи сервисов**:
  ```bash
  docker compose logs <service_name>  # Например: fraud_detector, writer, interface, kafka
  ```

## 🛠️ Использование

### 1. Загрузка данных:

 - Загрузите CSV через интерфейс Streamlit и нажмите кнопку отправки. Для тестирования работы проекта используется файл формата `test.csv` из соревнования https://www.kaggle.com/competitions/teta-ml-1-2025
 - Пример структуры данных:
    ```csv
    transaction_time,amount,lat,lon,merchant_lat,merchant_lon,gender,...
    2023-01-01 12:30:00,150.50,40.7128,-74.0060,40.7580,-73.9855,M,...
    ```
 - Для первых тестов рекомендуется загружать небольшой семпл данных (до 100 транзакций) за раз, чтобы исполнение кода не заняло много времени.

### 2. Просмотр результатов:
 - Нажмите **Check results** в интерфейсе Streamlit — появятся последние 10 мошеннических транзакций и гистограмма скоров последних 100 транзакций.

### 3. Мониторинг:
 - **Kafka UI**: Просматривайте сообщения в топиках `transactions` и `scoring`
 - **Логи обработки**:
   - `/app/logs/service.log` внутри контейнера `fraud_detector`
   - `/writer/logs/service.log` внутри контейнера `writer`

### 4. Формат результатов:

 - Скоринговые оценки пишутся в топик `scoring` в формате:
    ```json
    {
    "score": 0.995,
    "fraud_flag": 1,
    "transaction_id": "d6b0f7a0-8e1a-4a3c-9b2d-5c8f9d1e2f3a"
    }
    ```
 - `writer` сохраняет их в таблицу `scores`:
    ```sql
    CREATE TABLE scores (
        id             BIGSERIAL PRIMARY KEY,      -- порядок поступления
        transaction_id TEXT UNIQUE NOT NULL,
        score          DOUBLE PRECISION NOT NULL,
        fraud_flag     BOOLEAN NOT NULL
    );
    ```

## Структура проекта
```
.
├── fraud_detector/
│   ├── app/
│   │   └── app.py              # Kafka Consumer/Producer
│   ├── src/
│   │   ├── preprocessing.py    # Логика препроцессинга
│   │   └── scorer.py           # ML-модель и предсказания
│   ├── models/
│   │   └── model.cbm           # Предобученная модель CatBoost
│   ├── requirements.txt
│   └── Dockerfile
├── writer/
│   ├── write.py                # Kafka Consumer → PostgreSQL
│   ├── requirements.in
│   ├── requirements.txt
│   └── Dockerfile
├── interface/
│   ├── .streamlit/config.toml  # Конфигурация Streamlit
│   ├── app.py                  # Streamlit UI
│   ├── requirements.txt
│   └── Dockerfile
├── docker-compose.yaml
└── README.md
```

## Настройки Kafka
```yml
Топики:
- transactions (входные данные)
- scoring (результаты скоринга)

Репликация: 1 (для разработки)
Партиции: 3
Хранение: 7 дней (retention.ms=604800000)
```
