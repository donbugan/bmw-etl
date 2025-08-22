FROM python:3.10-slim

WORKDIR /app

COPY producer.py consumer.py ./

RUN pip install --no-cache-dir kafka-python psycopg2-binary

CMD ["python", "producer.py"] 
