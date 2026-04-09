"""
Kafka-Spark-Cassandra Streaming Pipeline
=========================================

This module implements a real-time data streaming pipeline that:
1. Consumes user data from a Kafka topic
2. Processes it using Spark Structured Streaming
3. Persists the data to a Cassandra database

Architecture:
    Kafka (users_created topic) -> Spark Streaming -> Cassandra (spark_streams.created_users)

Usage:
    python kafka_spark_streaming.py

Requirements:
    - Running Kafka broker with 'users_created' topic
    - Running Cassandra instance
    - Spark with Kafka and Cassandra connectors
"""

from dataclasses import dataclass
from typing import Optional
import uuid

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

from utils.logging_utils import logger


# Configuration
@dataclass
class CassandraConfig:
    """
    Cassandra connection configuration.
    
    Attributes:
        host: Cassandra cluster host address
        port: Cassandra native transport port
        keyspace: Target keyspace name
        table: Target table name
        username: Authentication username (optional)
        password: Authentication password (optional)
    """
    host: str = "localhost"
    port: int = 9042
    keyspace: str = "spark_streams"
    table: str = "created_users"
    username: Optional[str] = None
    password: Optional[str] = None


@dataclass
class KafkaConfig:
    """
    Kafka connection configuration.
    
    Attributes:
        bootstrap_servers: Kafka broker addresses
        topic: Topic to subscribe to
        starting_offsets: Where to start reading ('earliest', 'latest')
    """
    bootstrap_servers: str = "localhost:9092"
    topic: str = "users_created"
    starting_offsets: str = "earliest"


@dataclass
class SparkConfig:
    """
    Spark application configuration.
    
    Attributes:
        app_name: Spark application name
        log_level: Spark log level (INFO, WARN, ERROR)
        checkpoint_location: Directory for streaming checkpoints
    """
    app_name: str = "KafkaSparkStreaming"
    log_level: str = "INFO"
    checkpoint_location: str = "/tmp/checkpoint"


# Schema Definition
class UserSchema:
    """
    Defines the schema for user data consumed from Kafka.
    
    This schema maps to the JSON structure produced by the upstream
    data generator and matches the Cassandra table structure.
    """
    
    @staticmethod
    def get_schema() -> StructType:
        """
        Returns the Spark StructType schema for user data.
        
        Returns:
            StructType: Schema with all user fields
        """
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("first_name", StringType(), nullable=False),
            StructField("last_name", StringType(), nullable=False),
            StructField("gender", StringType(), nullable=True),
            StructField("address", StringType(), nullable=True),
            StructField("post_code", StringType(), nullable=True),
            StructField("email", StringType(), nullable=False),
            StructField("username", StringType(), nullable=False),
            StructField("registered_date", StringType(), nullable=True),
            StructField("phone", StringType(), nullable=True),
            StructField("picture", StringType(), nullable=True),
        ])


# Cassandra Manager
class CassandraManager:
    """
    Manages Cassandra database connections and operations.
    
    Handles keyspace creation, table creation, and data insertion
    for the streaming pipeline.
    
    Example:
        config = CassandraConfig(host="localhost")
        manager = CassandraManager(config)
        
        if manager.connect():
            manager.setup_schema()
            manager.insert_user(user_data)
            manager.close()
    """
    
    def __init__(self, config: CassandraConfig):
        """
        Initialize CassandraManager with configuration.
        
        Args:
            config: CassandraConfig instance with connection details
        """
        self.config = config
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None
    
    def connect(self) -> bool:
        """
        Establish connection to Cassandra cluster.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            # Configure authentication if credentials provided
            auth_provider = None
            if self.config.username and self.config.password:
                auth_provider = PlainTextAuthProvider(
                    username=self.config.username,
                    password=self.config.password
                )
            
            self.cluster = Cluster(
                contact_points=[self.config.host],
                port=self.config.port,
                auth_provider=auth_provider
            )
            self.session = self.cluster.connect()
            
            logger.info(f"Connected to Cassandra at {self.config.host}:{self.config.port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            return False
    
    def setup_schema(self) -> bool:
        """
        Create keyspace and table if they don't exist.
        
        Returns:
            bool: True if schema setup successful, False otherwise
        """
        try:
            self._create_keyspace()
            self._create_table()
            return True
        except Exception as e:
            logger.error(f"Failed to setup schema: {e}")
            return False
    
    def _create_keyspace(self) -> None:
        """Create the keyspace with SimpleStrategy replication."""
        query = f"""
            CREATE KEYSPACE IF NOT EXISTS {self.config.keyspace}
            WITH replication = {{
                'class': 'SimpleStrategy',
                'replication_factor': '1'
            }};
        """
        self.session.execute(query)
        logger.info(f"Keyspace '{self.config.keyspace}' is ready")
    
    def _create_table(self) -> None:
        """Create the users table with appropriate schema."""
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.config.keyspace}.{self.config.table} (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                post_code TEXT,
                email TEXT,
                username TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT
            );
        """
        self.session.execute(query)
        logger.info(f"Table '{self.config.keyspace}.{self.config.table}' is ready")
    
    def insert_user(self, user_data: dict) -> bool:
        """
        Insert a single user record into Cassandra.
        
        Args:
            user_data: Dictionary containing user fields
            
        Returns:
            bool: True if insertion successful, False otherwise
        """
        query = f"""
            INSERT INTO {self.config.keyspace}.{self.config.table}
            (id, first_name, last_name, gender, address, post_code,
             email, username, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            self.session.execute(query, (
                uuid.UUID(user_data.get('id')),
                user_data.get('first_name'),
                user_data.get('last_name'),
                user_data.get('gender'),
                user_data.get('address'),
                user_data.get('post_code'),
                user_data.get('email'),
                user_data.get('username'),
                user_data.get('registered_date'),
                user_data.get('phone'),
                user_data.get('picture'),
            ))
            logger.info(f"Inserted user: {user_data.get('first_name')} {user_data.get('last_name')}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert user: {e}")
            return False
    
    def close(self) -> None:
        """Close the Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")


# Spark Streaming Manager
class SparkStreamingManager:
    """
    Manages Spark session and streaming operations.
    
    Handles Spark session creation, Kafka stream consumption,
    and data transformation for the pipeline.
    
    Example:
        spark_config = SparkConfig()
        kafka_config = KafkaConfig()
        cassandra_config = CassandraConfig()
        
        manager = SparkStreamingManager(spark_config, kafka_config, cassandra_config)
        
        if manager.create_session():
            df = manager.read_kafka_stream()
            manager.write_to_cassandra(df)
    """
    
    # Spark package dependencies
    SPARK_PACKAGES = [
        "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
    ]
    
    def __init__(
        self,
        spark_config: SparkConfig,
        kafka_config: KafkaConfig,
        cassandra_config: CassandraConfig
    ):
        """
        Initialize SparkStreamingManager with configurations.
        
        Args:
            spark_config: Spark application settings
            kafka_config: Kafka connection settings
            cassandra_config: Cassandra connection settings
        """
        self.spark_config = spark_config
        self.kafka_config = kafka_config
        self.cassandra_config = cassandra_config
        self.spark: Optional[SparkSession] = None
    
    def create_session(self) -> bool:
        """
        Create and configure Spark session.
        
        Initializes SparkSession with required packages for
        Kafka and Cassandra connectivity.
        
        Returns:
            bool: True if session created successfully, False otherwise
        """
        try:
            packages = ",".join(self.SPARK_PACKAGES)
            
            self.spark = (
                SparkSession.builder
                .appName(self.spark_config.app_name)
                .config("spark.jars.packages", packages)
                .config("spark.cassandra.connection.host", self.cassandra_config.host)
                .config("spark.cassandra.connection.port", self.cassandra_config.port)
                .getOrCreate()
            )
            
            self.spark.sparkContext.setLogLevel(self.spark_config.log_level)
            
            logger.info(f"Spark session '{self.spark_config.app_name}' created")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            return False
    
    def read_kafka_stream(self) -> Optional[DataFrame]:
        """
        Create a streaming DataFrame from Kafka topic.
        
        Returns:
            DataFrame: Spark streaming DataFrame, or None on failure
        """
        if not self.spark:
            logger.error("Spark session not initialized")
            return None
        
        try:
            df = (
                self.spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_config.bootstrap_servers)
                .option("subscribe", self.kafka_config.topic)
                .option("startingOffsets", self.kafka_config.starting_offsets)
                .load()
            )
            
            logger.info(f"Kafka stream connected to topic '{self.kafka_config.topic}'")
            return df
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return None
    
    def transform_stream(self, raw_df: DataFrame) -> DataFrame:
        """
        Transform raw Kafka messages into structured user data.
        
        Parses JSON payload from Kafka message value and extracts
        fields according to the user schema.
        
        Args:
            raw_df: Raw Kafka streaming DataFrame
            
        Returns:
            DataFrame: Transformed DataFrame with user fields
        """
        schema = UserSchema.get_schema()
        
        transformed_df = (
            raw_df
            .selectExpr("CAST(value AS STRING) as json_value")
            .select(from_json(col("json_value"), schema).alias("data"))
            .select("data.*")
        )
        
        logger.info("Stream transformation configured")
        return transformed_df
    
    def write_to_cassandra(self, df: DataFrame) -> None:
        """
        Write streaming DataFrame to Cassandra.
        
        Starts a streaming query that continuously writes
        data to the configured Cassandra table.
        
        Args:
            df: Transformed streaming DataFrame
        """
        query = (
            df.writeStream
            .format("org.apache.spark.sql.cassandra")
            .option("checkpointLocation", self.spark_config.checkpoint_location)
            .option("keyspace", self.cassandra_config.keyspace)
            .option("table", self.cassandra_config.table)
            .start()
        )
        
        logger.info("Streaming query started - writing to Cassandra")
        logger.info(f"Checkpoint location: {self.spark_config.checkpoint_location}")
        
        # Block until termination
        query.awaitTermination()
    
    def stop(self) -> None:
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


# Main Pipeline Orchestrator
class StreamingPipeline:
    """
    Orchestrates the complete Kafka-Spark-Cassandra streaming pipeline.
    
    This is the main entry point that coordinates all components:
    1. Establishes Cassandra connection and creates schema
    2. Initializes Spark session with required configurations
    3. Connects to Kafka and starts consuming messages
    4. Transforms and writes data to Cassandra continuously
    
    Example:
        # Using default configurations
        pipeline = StreamingPipeline()
        pipeline.run()
        
        # Using custom configurations
        pipeline = StreamingPipeline(
            cassandra_config=CassandraConfig(host="cassandra-server"),
            kafka_config=KafkaConfig(bootstrap_servers="kafka:9092"),
            spark_config=SparkConfig(app_name="MyPipeline")
        )
        pipeline.run()
    """
    
    def __init__(
        self,
        cassandra_config: Optional[CassandraConfig] = None,
        kafka_config: Optional[KafkaConfig] = None,
        spark_config: Optional[SparkConfig] = None
    ):
        """
        Initialize pipeline with optional custom configurations.
        
        Args:
            cassandra_config: Cassandra settings (uses defaults if None)
            kafka_config: Kafka settings (uses defaults if None)
            spark_config: Spark settings (uses defaults if None)
        """
        self.cassandra_config = cassandra_config or CassandraConfig()
        self.kafka_config = kafka_config or KafkaConfig()
        self.spark_config = spark_config or SparkConfig()
        
        self.cassandra_manager: Optional[CassandraManager] = None
        self.spark_manager: Optional[SparkStreamingManager] = None
    
    def run(self) -> None:
        """
        Execute the streaming pipeline.
        
        This method:
        1. Sets up Cassandra schema
        2. Creates Spark session
        3. Connects to Kafka
        4. Starts continuous streaming to Cassandra
        
        The method blocks until the streaming query terminates
        or an error occurs.
        """

        logger.info("Starting Kafka-Spark-Cassandra Streaming Pipeline")
        try:
            # Step 1: Setup Cassandra
            if not self._setup_cassandra():
                raise RuntimeError("Cassandra setup failed")
            
            # Step 2: Setup Spark
            if not self._setup_spark():
                raise RuntimeError("Spark setup failed")
            
            # Step 3: Start streaming
            self._start_streaming()
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
            
        finally:
            self._cleanup()
    
    def _setup_cassandra(self) -> bool:
        """Initialize Cassandra connection and schema."""
        logger.info("Setting up Cassandra...")
        
        self.cassandra_manager = CassandraManager(self.cassandra_config)
        
        if not self.cassandra_manager.connect():
            return False
        
        if not self.cassandra_manager.setup_schema():
            return False
        
        return True
    
    def _setup_spark(self) -> bool:
        """Initialize Spark session."""
        logger.info("Setting up Spark...")
        
        self.spark_manager = SparkStreamingManager(
            self.spark_config,
            self.kafka_config,
            self.cassandra_config
        )
        
        return self.spark_manager.create_session()
    
    def _start_streaming(self) -> None:
        """Connect to Kafka and start streaming to Cassandra."""
        logger.info("Starting stream processing...")
        
        # Read from Kafka
        raw_stream = self.spark_manager.read_kafka_stream()
        if raw_stream is None:
            raise RuntimeError("Failed to create Kafka stream")
        
        # Transform data
        transformed_stream = self.spark_manager.transform_stream(raw_stream)
        
        # Write to Cassandra (blocks until termination)
        self.spark_manager.write_to_cassandra(transformed_stream)
    
    def _cleanup(self) -> None:
        """Clean up resources."""
        logger.info("Cleaning up resources...")
        
        if self.cassandra_manager:
            self.cassandra_manager.close()
        
        if self.spark_manager:
            self.spark_manager.stop()

if __name__ == "__main__":
    cassandra_config = CassandraConfig(
        host="localhost",
        keyspace="spark_streams",
        table="created_users"
    )
    
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",  # "broker:29092" in Docker
        topic="users_created"
    )
    
    spark_config = SparkConfig(
        app_name="UserStreamingPipeline",
        checkpoint_location="/tmp/checkpoint"
    )
    
    # Run the pipeline
    pipeline = StreamingPipeline(
        cassandra_config=cassandra_config,
        kafka_config=kafka_config,
        spark_config=spark_config
    )
    pipeline.run()