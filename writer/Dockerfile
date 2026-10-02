FROM python:3.12-slim

WORKDIR /writer

# Создание директории для логов
RUN mkdir -p logs && \
    touch logs/service.log && \
    chmod -R 777 logs  # Права на запись для всех пользователей

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY . .

CMD ["python", "write.py"]