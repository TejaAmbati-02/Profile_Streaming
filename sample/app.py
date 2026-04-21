from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import getpass
username = getpass.getuser()
spark = SparkSession.builder \
        .config("spark.shuffle.useOldFetchProtocol", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer") \
        .config("spark.sql.warehouse.dir", f"/user/{username}/warehouse") \
        .enableHiveSupport() \
        .master('yarn') \
        .appName('demo_streaming') \
        .getOrCreate()


# Create Dataframe representing the stream of input lines from connection to localhost:9998

lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9998).load()
# Split the lines into words
words = lines.select(explode(split(lines.value, " ")).alias("word"))

#Generate running word count
wordCounts = words.groupBy("word").count()

# Start running the query that prints the running counts to the console
query = wordCounts.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()