FROM python:3.10-slim

WORKDIR /app

COPY producer.py consumer.py ./
COPY .env ./

RUN pip install --no-cache-dir kafka-python psycopg2-binary python-dotenv

CMD ["python", "producer.py"]
