from datetime import datetime


from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

from utils.logging_utils import logger, log_function


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
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
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')
    
    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logger.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logger.error(f'could not insert data due to {e}')



def create_spark_connection():
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("KafkaSparkStreaming") \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("INFO")
        logger.info("Spark connection created successfully!")
    
    except Exception as e:
        logger.error(f'could not create spark connection due to {e}')
    
    return spark



def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn \
            .readStream \
            .format("kafka") \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option("subscribe", "users_created") \
            .option("startingOffsets", "earliest") \
            .load()

        logger.info("kafka dataframe created successfully")

    except Exception as e:
        logger.warning(f"kafka dataframe could not be created because: {e}")
    return spark_df



def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        logger.info("Cassandra connection created successfully!")
        return cas_session
    except Exception as e:
        logger.error(f"Could not create cassandra connection due to {e}")
        return None
    
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    logger.info(f"Selected DataFrame:{sel}")
    # logger.info(f"Selected DataFrame:{sel.show()}")
    print(sel)
    return sel



if __name__ == "__main__":
    # create spark connection
    spark_connection = create_spark_connection()

    if spark_connection is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_connection)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logger.info("Streaming is being started...")
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()