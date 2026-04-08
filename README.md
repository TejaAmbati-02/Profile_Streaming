# Profile Streaming Data Pipeline

A production-oriented streaming data pipeline demonstrating scalability, fault tolerance, and modular architecture patterns.

## Architecture

The system follows a decoupled, event-driven architecture:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Random     │    │   Apache    │    │   Apache    │    │   Apache    │    │   Apache    │
│  User API   │───▶│   Airflow   │───▶│   Kafka     │───▶│   Spark     │───▶│  Cassandra  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                         │                  │
                   ┌─────┴─────┐      ┌─────┴─────┐
                   │ PostgreSQL│      │ Zookeeper │
                   │ (metadata)│      │  Schema   │
                   └───────────┘      │ Registry  │
                                      │  Control  │
                                      │  Center   │
                                      └───────────┘
```

### Components

| Layer | Technology | Purpose |
|-------|------------|---------|
| Ingestion | Python + Airflow | Collect data from external API, schedule jobs |
| Messaging | Kafka + Zookeeper | Distributed event streaming, decoupling |
| Metadata | Schema Registry | Kafka message schema management |
| Monitoring | Control Center | Kafka topic and message monitoring UI |
| Processing | Spark Streaming | Near real-time transformations |
| Storage | Cassandra | High-throughput, low-latency writes |
| Infrastructure | PostgreSQL | Airflow metadata database |
| Deployment | Docker / Podman | Containerized services |

## Design Decisions

**Why Kafka?**  
Durability, partitioning, and high-throughput event streaming. Enables loose coupling between producers and consumers.

**Why Spark Streaming over single-node solutions?**  
Distributed processing with horizontal scalability. Handles transformations, filtering, and enrichment across nodes.

**Why Cassandra?**  
Optimized for write-heavy workloads. Horizontal scaling suits streaming ingestion patterns.

**Why Airflow alongside Kafka?**  
Different responsibilities. Kafka handles event streaming; Airflow manages orchestration, retries, and scheduling.

## Data Flow

1. External API emits events (simulated user profile data)
2. Airflow DAG triggers, fetches data, publishes to Kafka
3. Kafka streams events to the `users_created` topic
4. Spark consumes the stream, applies schema, transforms data
5. Cassandra stores processed results

## Tech Stack

- Python 3.9 - 3.11 (Cassandra driver requirement)
- Apache Airflow
- Apache Kafka
- Apache Zookeeper
- Schema Registry
- Confluent Control Center
- Apache Spark (Master-Worker)
- Apache Cassandra
- PostgreSQL (Airflow backend)
- Docker / Podman

## Prerequisites

### System Requirements

- Minimum 8GB RAM (Spark workers require 1GB each)
- Docker Engine 20.10+ / Podman 4.0+
- Docker Compose v2+ / podman-compose 1.0+
- Python 3.9 - 3.11

### Required Ports

| Port | Service |
|------|---------|
| 8080 | Airflow UI |
| 9021 | Kafka Control Center |
| 9092 | Kafka Broker |
| 8081 | Spark Master UI |
| 9042 | Cassandra |
| 5432 | PostgreSQL |

## Project Structure

```
.
├── dags/
│   └── kafka_stream.py        # Airflow DAG for API ingestion
├── scripts/
│   └── spark_stream.py        # Spark streaming job
├── docker-compose.yaml        # Container orchestration
├── requirements.txt
└── README.md
```

## Setup

### 1. Clone Repository

```bash
git clone <your-repo-url>
cd <your-project-folder>
```

### 2. Start Services

**Docker:**
```bash
docker-compose up -d
```

**Podman:**
```bash
podman compose up -d
```

Wait 2-3 minutes for all services to initialize.

### 3. Verify Services

| Service | URL / Command |
|---------|---------------|
| Airflow UI | http://localhost:8080 |
| Kafka Control Center | http://localhost:9021 |
| Spark Master UI | http://localhost:8081 |

```bash
# Check Kafka topics
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Check Cassandra
docker exec -it cassandra cqlsh -e "DESCRIBE KEYSPACES;"
```

### 4. Create Cassandra Keyspace and Table

```bash
docker exec -it cassandra cqlsh
```

```sql
CREATE KEYSPACE IF NOT EXISTS spark_streams
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS spark_streams.users (
    id UUID PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    address TEXT,
    city TEXT,
    country TEXT,
    postcode TEXT,
    email TEXT,
    username TEXT,
    dob TEXT,
    registered_date TEXT,
    phone TEXT,
    picture TEXT
);
```

### 5. Run the Pipeline

1. Open Airflow UI → Enable and trigger the DAG
2. Monitor the `users_created` topic in Control Center
3. Submit the Spark streaming job:

```bash
docker exec -it spark-master spark-submit \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  /opt/bitnami/spark/jobs/spark_stream.py
```

### 6. Verify Data

```bash
docker exec -it cassandra cqlsh -e "SELECT * FROM spark_streams.users LIMIT 5;"
```

## Useful Commands

| Action | Docker | Podman |
|--------|--------|--------|
| List containers | `docker ps` | `podman ps` |
| Enter container | `docker exec -it <name> bash` | `podman exec -it <name> bash` |
| View logs | `docker-compose logs -f` | `podman compose logs -f` |
| Stop services | `docker-compose down` | `podman compose down` |
| Remove volumes | `docker-compose down -v` | `podman compose down -v` |
| Rebuild | `docker-compose up -d --build` | `podman compose up -d --build` |
| System prune | `docker system prune -a` | `podman system prune -a` |

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Kafka broker not available | Ensure Zookeeper is healthy: `docker logs zookeeper` |
| Spark job OOM error | Increase worker memory in `docker-compose.yaml` (min 1GB) |
| Cassandra connection refused | Wait 60s after container start; check `docker logs cassandra` |
| Schema Registry errors | Kafka must be fully up before Schema Registry starts |
| DAG not visible in Airflow | Verify file is in `dags/` folder with no syntax errors |
| Python/Cassandra driver issues | Confirm Python version is 3.9 - 3.11 |

## Challenges & Learnings

- Managing service dependencies in distributed setups
- Data consistency across multiple systems
- Trade-offs between batch and streaming approaches
- Debugging failures in asynchronous pipelines

## Future Improvements

- [ ] Monitoring with Prometheus & Grafana
- [ ] Data validation and anomaly detection
- [ ] Cloud deployment (AWS/GCP)
- [ ] CI/CD pipeline for DAG testing
- [ ] Dead letter queue for failed messages

## Notes

- The Random User API has rate limits — avoid aggressive polling intervals
- Spark Streaming uses micro-batches; expect ~1-2s latency
- For production: add authentication, increase replication factors, configure proper resource limits
