"""
Kafka User Data Producer DAG
============================

An Airflow DAG that fetches random user data from an external API
and streams it to a Kafka topic for downstream processing.

Pipeline Flow:
    RandomUser API -> Format -> Kafka (users_created topic)

The DAG runs daily and streams data continuously for a configured
duration (default: 60 seconds) per run.

Usage:
    - Deploy to Airflow DAGs folder
    - Enable the 'user_data_kafka_stream' DAG in Airflow UI
    - Can also run standalone: python kafka_producer_dag.py
"""

import json
import sys
import uuid
import time
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional, Any
from abc import ABC, abstractmethod

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add project root to path for local imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# from utils.logging_utils import logger, log_function



# Configuration
@dataclass
class KafkaProducerConfig:
    """
    Kafka producer configuration settings.
    
    Attributes:
        bootstrap_servers: List of Kafka broker addresses
        topic: Target Kafka topic name
        max_block_ms: Max time to block waiting for buffer space
        acks: Number of acknowledgments required ('all', 0, 1)
        retries: Number of retries on failure
    """
    bootstrap_servers: list[str] = field(default_factory=lambda: ["broker:29092"])
    topic: str = "users_created"
    max_block_ms: int = 5000
    acks: str = "all"
    retries: int = 3


@dataclass
class StreamingConfig:
    """
    Streaming job configuration.
    
    Attributes:
        duration_seconds: How long to stream data per run
        delay_between_messages: Delay between API calls (rate limiting)
        max_errors: Stop streaming after this many consecutive errors
    """
    duration_seconds: int = 60
    delay_between_messages: float = 0.5
    max_errors: int = 10


@dataclass
class APIConfig:
    """
    External API configuration.
    
    Attributes:
        base_url: API endpoint URL
        timeout: Request timeout in seconds
    """
    base_url: str = "https://randomuser.me/api/"
    timeout: int = 10



# Data Models
@dataclass
class UserData:
    """
    Structured user data model.
    
    Represents a formatted user record ready for Kafka publishing.
    All fields are strings for JSON serialization compatibility.
    """
    id: str
    first_name: str
    last_name: str
    gender: str
    address: str
    post_code: str
    email: str
    username: str
    dob: str
    registered_date: str
    phone: str
    picture: str
    
    def to_dict(self) -> dict[str, str]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "gender": self.gender,
            "address": self.address,
            "post_code": self.post_code,
            "email": self.email,
            "username": self.username,
            "dob": self.dob,
            "registered_date": self.registered_date,
            "phone": self.phone,
            "picture": self.picture,
        }
    
    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict())



# Data Source (API Client)
class DataSource(ABC):
    """Abstract base class for data sources."""
    
    @abstractmethod
    def fetch(self) -> dict[str, Any]:
        """Fetch raw data from source."""
        pass


class RandomUserAPIClient(DataSource):
    """
    Client for the RandomUser.me API.
    
    Fetches random user data for testing and demonstration purposes.
    
    Example:
        client = RandomUserAPIClient()
        raw_data = client.fetch()
    """
    
    def __init__(self, config: Optional[APIConfig] = None):
        """
        Initialize API client.
        
        Args:
            config: API configuration (uses defaults if None)
        """
        self.config = config or APIConfig()
    
    # @log_function
    def fetch(self) -> dict[str, Any]:
        """
        Fetch a random user from the API.
        
        Returns:
            dict: Raw user data from API response
            
        Raises:
            requests.RequestException: On network or API errors
        """
        response = requests.get(
            self.config.base_url,
            timeout=self.config.timeout
        )
        response.raise_for_status()
        
        data = response.json()
        user_data = data["results"][0]
        
        logger.info("Fetched user data from API")
        return user_data



# Data Transformer


class UserDataTransformer:
    """
    Transforms raw API responses into structured UserData objects.
    
    Handles data extraction, formatting, and ID generation for
    user records before publishing to Kafka.
    
    Example:
        transformer = UserDataTransformer()
        user = transformer.transform(raw_api_response)
    """
    
    @staticmethod
    # @log_function
    def transform(raw_data: dict[str, Any]) -> UserData:
        """
        Transform raw API data into a UserData object.
        
        Args:
            raw_data: Raw user data from RandomUser API
            
        Returns:
            UserData: Structured and formatted user record
        """
        location = raw_data["location"]
        
        # Build formatted address
        address = (
            f"{location['street']['number']} {location['street']['name']}, "
            f"{location['city']}, {location['state']}, {location['country']}"
        )
        
        user = UserData(
            id=str(uuid.uuid4()),
            first_name=raw_data["name"]["first"],
            last_name=raw_data["name"]["last"],
            gender=raw_data["gender"],
            address=address,
            post_code=str(location["postcode"]),
            email=raw_data["email"],
            username=raw_data["login"]["username"],
            dob=raw_data["dob"]["date"],
            registered_date=raw_data["registered"]["date"],
            phone=raw_data["phone"],
            picture=raw_data["picture"]["medium"],
        )
        
        # logger.info(
        #     f"Transformed user data",
        #     extra={"user_id": user.id, "username": user.username}
        # )
        return user



# Kafka Publisher
class KafkaPublisher:
    """
    Publishes messages to Kafka topics.
    
    Manages Kafka producer lifecycle and provides methods for
    publishing user data with proper error handling.
    
    Example:
        config = KafkaProducerConfig(bootstrap_servers=["localhost:9092"])
        publisher = KafkaPublisher(config)
        
        try:
            publisher.connect()
            publisher.publish(user_data)
        finally:
            publisher.close()
    """
    
    def __init__(self, config: Optional[KafkaProducerConfig] = None):
        """
        Initialize Kafka publisher.
        
        Args:
            config: Kafka producer configuration (uses defaults if None)
        """
        self.config = config or KafkaProducerConfig()
        self.producer: Optional[KafkaProducer] = None
    
    def connect(self) -> bool:
        """
        Create Kafka producer connection.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                max_block_ms=self.config.max_block_ms,
                acks=self.config.acks,
                retries=self.config.retries,
                value_serializer=lambda v: v.encode("utf-8"),
            )
            logger.info(
                f"Connected to Kafka brokers: {self.config.bootstrap_servers}"
            )
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def publish(self, user: UserData) -> bool:
        """
        Publish user data to Kafka topic.
        
        Args:
            user: UserData object to publish
            
        Returns:
            bool: True if published successfully, False otherwise
        """
        if not self.producer:
            # logger.error("Producer not connected")
            return False
        
        try:
            future = self.producer.send(
                self.config.topic,
                value=user.to_json()
            )
            # Wait for confirmation
            future.get(timeout=10)
            
            # logger.info(
            #     f"Published user to Kafka",
            #     extra={"user_id": user.id, "topic": self.config.topic}
            # )
            return True
            
        except KafkaError as e:
            # logger.error(f"Failed to publish to Kafka: {e}")
            return False
    
    def flush(self) -> None:
        """Flush any pending messages."""
        if self.producer:
            self.producer.flush()
    
    def close(self) -> None:
        """Close the Kafka producer connection."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            # logger.info("Kafka producer closed")



# Streaming Job
class UserDataStreamingJob:
    """
    Orchestrates the user data streaming pipeline.
    
    Coordinates fetching data from the API, transforming it,
    and publishing to Kafka for a configured duration.
    
    Example:
        job = UserDataStreamingJob()
        job.run()  # Streams for configured duration
    """
    
    def __init__(
        self,
        api_config: Optional[APIConfig] = None,
        kafka_config: Optional[KafkaProducerConfig] = None,
        streaming_config: Optional[StreamingConfig] = None,
    ):
        """
        Initialize streaming job.
        
        Args:
            api_config: API client configuration
            kafka_config: Kafka producer configuration
            streaming_config: Streaming job configuration
        """
        self.streaming_config = streaming_config or StreamingConfig()
        
        self.api_client = RandomUserAPIClient(api_config)
        self.transformer = UserDataTransformer()
        self.publisher = KafkaPublisher(kafka_config)
        
        # Statistics
        self.messages_sent = 0
        self.errors = 0
    
    @log_function
    def run(self) -> dict[str, int]:
        """
        Execute the streaming job.
        
        Fetches and publishes user data continuously until:
        - Duration limit reached
        - Max consecutive errors exceeded
        
        Returns:
            dict: Statistics with 'messages_sent' and 'errors' counts
        """

        # logger.info("Starting User Data Streaming Job")
        # logger.info(f"Duration: {self.streaming_config.duration_seconds}s")

        
        if not self.publisher.connect():
            raise RuntimeError("Failed to connect to Kafka")
        
        try:
            self._stream_loop()
        finally:
            self.publisher.close()
        
        # logger.info(
        #     f"Streaming complete. Sent: {self.messages_sent}, Errors: {self.errors}"
        # )
        
        return {
            "messages_sent": self.messages_sent,
            "errors": self.errors,
        }
    
    def _stream_loop(self) -> None:
        """Main streaming loop."""
        start_time = time.time()
        end_time = start_time + self.streaming_config.duration_seconds
        consecutive_errors = 0
        
        while time.time() < end_time:
            # Check error threshold
            if consecutive_errors >= self.streaming_config.max_errors:
                # logger.error(
                #     f"Stopping: {consecutive_errors} consecutive errors"
                # )
                break
            
            try:
                # Fetch -> Transform -> Publish
                raw_data = self.api_client.fetch()
                user = self.transformer.transform(raw_data)
                
                if self.publisher.publish(user):
                    self.messages_sent += 1
                    consecutive_errors = 0
                else:
                    self.errors += 1
                    consecutive_errors += 1
                
            except requests.RequestException as e:
                # logger.error(f"API request failed: {e}")
                self.errors += 1
                consecutive_errors += 1
                
            except Exception as e:
                # logger.error(f"Unexpected error: {e}")
                self.errors += 1
                consecutive_errors += 1
            
            # Rate limiting
            time.sleep(self.streaming_config.delay_between_messages)



# Airflow DAG Definition
def run_streaming_job() -> None:
    """
    Airflow task callable that runs the streaming job.
    
    Uses Docker-compatible configuration (broker:29092).
    """
    kafka_config = KafkaProducerConfig(
        bootstrap_servers=["broker:29092"],
        topic="users_created",
    )
    
    streaming_config = StreamingConfig(
        duration_seconds=60,
        delay_between_messages=0.5,
    )
    
    job = UserDataStreamingJob(
        kafka_config=kafka_config,
        streaming_config=streaming_config,
    )
    job.run()


# DAG default arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# DAG definition
with DAG(
    dag_id="user_data_kafka_stream",
    default_args=default_args,
    description="Fetches random user data and streams to Kafka",
    schedule_interval="@daily",
    catchup=False,
    tags=["kafka", "streaming", "users"],
) as dag:
    
    streaming_task = PythonOperator(
        task_id="stream_users_to_kafka",
        python_callable=run_streaming_job,
        doc_md="""
        ### Stream Users to Kafka
        
        Fetches random user data from RandomUser.me API and
        publishes to the `users_created` Kafka topic.
        
        **Duration:** 60 seconds per run
        **Rate:** ~2 messages/second
        """,
    )



# Standalone Execution


if __name__ == "__main__":
    """
    Run the streaming job standalone (outside Airflow).
    
    Uses localhost configuration for local development.
    """
    # Local development configuration
    kafka_config = KafkaProducerConfig(
        bootstrap_servers=["localhost:9092"],
        topic="users_created",
    )
    
    streaming_config = StreamingConfig(
        duration_seconds=60,
        delay_between_messages=0.5,
    )
    
    job = UserDataStreamingJob(
        kafka_config=kafka_config,
        streaming_config=streaming_config,
    )
    
    stats = job.run()
    # logger.info(f"Final Stats: {stats}")