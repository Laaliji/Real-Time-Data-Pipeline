
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, concat_ws, current_date, datediff, year, count, avg, split, expr, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from cassandra.cluster import Cluster
from pymongo import MongoClient

# Initialize Spark session
spark = SparkSession.builder\
        .appName("UserProfileAnalysis")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

# Define Kafka source configuration
kafka_source = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_profiles") \
    .load()

# Define the schema for parsing JSON data
json_schema = StructType([
    StructField("gender", StringType()),
    StructField("name", StructType([
        StructField("title", StringType()),
        StructField("first", StringType()),
        StructField("last", StringType())
    ])),
    StructField("location", StructType([
        StructField("street", StructType([
            StructField("number", IntegerType()),
            StructField("name", StringType())
        ])),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
        StructField("postcode", IntegerType())
    ])),
    StructField("email", StringType()),
    StructField("login", StructType([
        StructField("uuid", StringType()),
        StructField("username", StringType()),
    ])),
    StructField("dob", StructType([
        StructField("date", StringType()),
        StructField("age", IntegerType())
    ])),
    StructField("registered", StructType([
        StructField("date", StringType()),
        StructField("age", IntegerType())
    ])),
    StructField("phone", StringType()),
    StructField("nat", StringType())
])

# Parse JSON data using the defined schema
parsedStreamDF = kafka_source.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), json_schema).alias("data")) \
    .select("data.*")


# 1. Transformations
parsedStreamDF = parsedStreamDF.withColumn("full_name",
    concat_ws(" ",
        col("name.title"),
        col("name.first"),
        col("name.last")
    )
)
parsedStreamDF = parsedStreamDF.withColumn("calculated_age", year(current_date()) - year(to_date(parsedStreamDF["dob.date"])))
parsedStreamDF = parsedStreamDF.withColumn("complete_address",
    concat_ws(", ",
        col("location.street.number").cast("string"),
        col("location.street.name"),
        col("location.city"),
        col("location.state"),
        col("location.country"),
        col("location.postcode").cast("string")
    )
)

# Define the Cassandra cluster
cluster = Cluster(['localhost'],port=9042)
session = cluster.connect()

# Define the keyspace name
keyspace = "user_profiles"

# Define the table name
table_name = "users"

# Create keyspace and table if they don't exist
session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
session.execute(f"USE {keyspace}")

# Define the table with full_name as the primary key
table_creation_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        full_name TEXT PRIMARY KEY,
        calculated_age INT,
        complete_address TEXT
    )
"""
session.execute(table_creation_query)

# Select only the columns needed for Cassandra table
cassandraDF = parsedStreamDF.select("full_name", "calculated_age", "complete_address")


# Function to save DataFrame to Cassandra
def save_to_cassandra(df, keyspace, table_name, checkpoint_location):
    query = df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: batch_df.write \
                      .format("org.apache.spark.sql.cassandra") \
                      .mode("append") \
                      .option("keyspace", keyspace) \
                      .option("table", table_name) \
                      .option("checkpointLocation", checkpoint_location) \
                      .save())
    return query


# Start the streaming query
checkpoint_location = "./checkpoint/data"
query = save_to_cassandra(cassandraDF, keyspace, table_name, checkpoint_location)
query.start().awaitTermination()