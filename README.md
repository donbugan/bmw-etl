# Carparts ETL with Kafka, PostgreSQL, and Airflow

This project demonstrates an event-driven ETL pipeline for car parts data using **Kafka**, **PostgreSQL**, and **Apache Airflow**.  
Producers generate car part messages, Kafka brokers handle event streaming, consumers persist the data into PostgreSQL, and Airflow orchestrates workflows.

---

## 🚀 Architecture
- **Producer** (`producer.py`): Publishes car part events to Kafka.
- **Consumer** (`consumer.py`): Subscribes to Kafka topics and writes events into PostgreSQL.
- **PostgreSQL**: Stores normalized car parts data with role-based access control and audit logging.
- **Airflow**: Manages ETL workflows and schedules.
- **Docker Compose**: Spins up the full environment locally.

---

## Project Structure
```
.
├── LICENSE
├── README.md
├── airflow
│   ├── dags
│   │   ├── kafka_to_postgres.py
│   │   └── parts_pipeline.py
│   ├── init-airflow.sh
│   └── init-scheduler.sh
├── docker
│   └── kafka
│       ├── Dockerfile
│       ├── consumer.py
│       └── producer.py
├── docker-compose.yml
└── postgres
    └── init.sql
```

---

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/bmw-etl.git
cd bmw-etl
```

### 2. Start Services
```bash
docker compose up -d
```

#### This launches:
- Kafka broker & Zookeeper
- PostgreSQL database
- Airflow scheduler & webserver
- Producer and consumer containers

### 3. Verify Services

- **PostgreSQL**
```bash
psql -h localhost -U parts_admin -d carparts -c "\dt"
```

- **Kafka Topics**
```bash
docker exec -it bmw-etl-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 4. Usage

- **Check Producer Logs**
```bash
docker logs -f bmw-etl-kafka-producer-1
```

- **Check Consumer Logs**
```bash
docker logs -f bmw-etl-kafka-consumer-1
```

### 5. Query Data
```sql
SELECT * FROM parts LIMIT 10;
```

## Airflow

### 6. Access the Airflow UI
- URL: [http://localhost:8080](http://localhost:8080)
- **Default login:**  
  Username: admin  
  Password: admin

## Cleanup

```bash
docker compose down -v
```

## Notes
- Docker is configured with at least 8 GB RAM.
- If Airflow webserver crashes with PID errors:
```bash
docker compose rm -f bmw-etl-airflow-webserver-1
docker compose up -d bmw-etl-airflow-webserver-1
```

