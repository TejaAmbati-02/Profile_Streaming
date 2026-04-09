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
| 2181 | Zookeeper |
| 8080 | Airflow UI |
| 8081 | Schema Registry |
| 9021 | Kafka Control Center |
| 9090 | Spark Master UI |
| 9092 | Kafka Broker |
| 9042 | Cassandra |

### Service Startup Order

Services start in dependency chains. Docker Compose waits for health checks before starting dependent services.

```
           ┌─────────────────────────────────────────────────────────────┐
           │                    KAFKA CHAIN                              │
           │  zookeeper ──▶ broker ──▶ schema-registry ──▶ control-center│
           └─────────────────────────────────────────────────────────────┘

           ┌─────────────────────────────────────────────────────────────┐
           │                   AIRFLOW CHAIN                             │
           │              postgres ──▶ webserver ──▶ scheduler           │
           └─────────────────────────────────────────────────────────────┘

           ┌─────────────────────────────────────────────────────────────┐
           │                    SPARK CHAIN                              │
           │                spark-master ──▶ spark-worker                │
           └─────────────────────────────────────────────────────────────┘

           ┌─────────────────────────────────────────────────────────────┐
           │                    STANDALONE                               │
           │                     cassandra                               │
           └─────────────────────────────────────────────────────────────┘
```

**Startup sequence:**

1. **Phase 1 (parallel):** `zookeeper`, `postgres`, `spark-master`, `cassandra` — no dependencies, start immediately
2. **Phase 2:** `broker` starts after zookeeper is healthy
3. **Phase 3:** `schema-registry` and `webserver` start after broker and postgres are healthy (respectively)
4. **Phase 4:** `control-center`, `scheduler`, `spark-worker` start after their dependencies pass health checks

**Why this matters:**
- Kafka won't accept connections until Zookeeper confirms cluster coordination
- Schema Registry requires Kafka to store schemas
- Control Center needs both broker and Schema Registry to display topics and schemas
- Airflow webserver initializes the DB; scheduler runs migrations and starts after

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
| Spark Master UI | http://localhost:9090 |
| Schema Registry | http://localhost:8081 |

```bash
# Check Kafka topics
docker exec -it broker kafka-topics --list --bootstrap-server localhost:9092

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

CREATE TABLE IF NOT EXISTS spark_streams.created_users (
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

**Option A: Copy script and run inside container**
```bash
# Copy script to container
docker cp scripts/spark_stream.py spark-master:/opt/spark/

# Submit job
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark/spark_stream.py
```

**Option B: Add volume mount to docker-compose.yaml**
```yaml
spark-master:
  # ... existing config ...
  volumes:
    - ./scripts:/opt/spark/scripts
```

Then run:
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark/scripts/spark_stream.py
```

### 6. Verify Data in Cassandra

**Quick check:**
```bash
# Docker
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM spark_streams.created_users;"

# Podman
podman exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM spark_streams.created_users;"
```

**Detailed verification:**
```bash
# List all keyspaces
docker exec -it cassandra cqlsh -e "DESCRIBE KEYSPACES;"

# Check tables in spark_streams keyspace
docker exec -it cassandra cqlsh -e "USE spark_streams; DESCRIBE TABLES;"

# View table schema
docker exec -it cassandra cqlsh -e "DESCRIBE TABLE spark_streams.created_users;"

# Sample data
docker exec -it cassandra cqlsh -e "SELECT id, first_name, last_name, email FROM spark_streams.created_users LIMIT 10;"

# Row count
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM spark_streams.created_users;"
```

> **Note:** Replace `docker` with `podman` if using Podman.

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
| Spark job OOM error | Increase `SPARK_WORKER_MEMORY` in docker-compose (default: 1g) |
| Cassandra connection refused | Wait 60-90s after container start; check `docker logs cassandra` |
| Schema Registry errors | Broker must be fully healthy first; check `docker logs broker` |
| DAG not visible in Airflow | Verify file is in `dags/` folder with no syntax errors |
| Python/Cassandra driver issues | Confirm Python version is 3.9 - 3.11 |
| Control Center won't start | Requires both broker AND schema-registry healthy first |

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
