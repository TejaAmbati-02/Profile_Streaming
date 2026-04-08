# Profile Streaming Data Pipeline

A production-oriented streaming data pipeline demonstrating scalability, fault tolerance, and modular architecture patterns.

## Architecture

The system follows a decoupled, event-driven architecture:

```
External API → Airflow → PostgreSQL → Kafka → Spark Streaming → Cassandra
```

### Components

| Layer | Technology | Purpose |
|-------|------------|---------|
| Ingestion | Python + Airflow | Collect data from external API, schedule jobs |
| Staging | PostgreSQL | Store raw data for traceability |
| Messaging | Kafka + Zookeeper | Distributed event streaming, decoupling |
| Processing | Spark Streaming | Near real-time transformations |
| Storage | Cassandra | High-throughput, low-latency writes |
| Deployment | Docker | Containerized services |

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

1. External API emits events (simulated user-generated data)
2. Airflow schedules ingestion, stages raw data in PostgreSQL
3. Kafka streams events to downstream consumers
4. Spark processes and enriches incoming data
5. Cassandra stores processed results

## Tech Stack

- Python
- Apache Airflow
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- PostgreSQL
- Docker

## Setup

### Prerequisites

**Docker:**
- Docker Engine 20.10+
- Docker Compose v2+

**Podman:**
- Podman 4.0+
- podman compose 1.0+

### Clone Repository

```bash
git clone <your-repo-url>
cd <your-project-folder>
```

### Start Services

**Using Docker:**
```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Rebuild after changes
docker-compose up -d --build
```

**Using Podman:**
```bash
# Start all services
podman compose up -d

# View logs
podman compose logs -f

# Stop services
podman compose down

# Rebuild after changes
podman compose up -d --build
```

### Useful Commands

| Action | Docker | Podman |
|--------|--------|--------|
| List containers | `docker ps` | `podman ps` |
| Enter container | `docker exec -it <name> bash` | `podman exec -it <name> bash` |
| View logs | `docker logs <name>` | `podman logs <name>` |
| Remove volumes | `docker-compose down -v` | `podman compose down -v` |
| System prune | `docker system prune -a` | `podman system prune -a` |

### Verify Services

```bash
# Check Kafka
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092
# or
podman exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Check Cassandra
docker exec -it cassandra cqlsh -e "DESCRIBE KEYSPACES;"
# or
podman exec -it cassandra cqlsh -e "DESCRIBE KEYSPACES;"
```

Access Airflow UI → Trigger the DAG → Monitor streaming jobs

## Challenges & Learnings

- Managing service dependencies in distributed setups
- Data consistency across multiple systems
- Trade-offs between batch and streaming approaches
- Debugging failures in asynchronous pipelines

## Future Improvements

- [ ] Monitoring with Prometheus & Grafana
- [ ] Schema registry for data governance
- [ ] Data validation and anomaly detection
- [ ] Cloud deployment (AWS/GCP)