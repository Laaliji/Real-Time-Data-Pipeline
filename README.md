# Development-of-a-Real-Time-Data-Pipeline-for-the-Analysis-of-User-Profiles

## I. Environment

To set up the required environment for this project, follow these steps:

### 1. Install Docker

If you don't already have Docker installed, download and install it from the official Docker website: [Get Docker](https://www.docker.com/get-started).

### 2. Pull Docker Images

Run this yml to pull the necessary Docker images for Kafka, Cassandra, and MongoDB with this code :
 - Code:
    ```bash
    docker-compose -f docker-compose.yml up -d

- docker-compose.yml :
  ```bash
    version: '3'

    services:
      zookeeper:
        image: wurstmeister/zookeeper
        container_name: zookeeper
        ports:
          - "2181:2181"
    
      kafka:
        image: wurstmeister/kafka
        container_name: kafka
        ports:
          - "9092:9092"
        environment:
          KAFKA_ADVERTISED_HOST_NAME: localhost
          KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    
      cassandra:
        image: cassandra
        container_name: cassandra
        ports:
          - "9042:9042"
    
      mongo:
        image: mongo
        container_name: mongo
        ports:
          - "27017:27017"
   

 - Installing and Configuring Spark
   ``` bash
    sudo apt install default-jdk
    wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
    tar xvf spark-3.5.0-bin-hadoop3.tgz
    sudo mv spark-3.5.0-bin-hadoop3 /opt/spark
    nano ~/.bashrc
    export SPARK_HOME=/opt/spark
    export PATH=$PATH:$SPARK_HOME/bin
    source ~/.bashrc
    spark-shell


## II. Start Kafka  

### 1. Access the Kafka Container
- Code:
  ```bash
  docker exec -it kafka /bin/sh  

### 2. Create a Kafka Topic

- Code:
  ```bash
  kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic user_profiles  

### 3. Verify the Topic Creation

- Code:
  ```bash
  kafka-topics.sh --list --zookeeper zookeeper:2181

### 4. Use the Kafka Console Producer

- Code:
  ```bash
  python3 producer.py
  
### 5. Use the Kafka Console Consumer

- Code:
  ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 consumer.py


## III. Transformations
- Code:
  ```bash
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

## IV. Start Cassandra

### Define the Cassandra cluster
- Code:
  ```bash
  cluster = Cluster(['localhost'],port=9042)
  session = cluster.connect()

### Define the keyspace name
- Code:
  ```bash
  keyspace = "user_profiles"

### Define the table name
- Code:
  ```bash
  table_name = "users"

### Create keyspace and table if they don't exist
- Code:
  ```bash
  session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
  session.execute(f"USE {keyspace}")

### Define the table with full_name as the primary key
- Code:
  ```bash
  table_creation_query = f"""
      CREATE TABLE IF NOT EXISTS {table_name} (
          full_name TEXT PRIMARY KEY,
          calculated_age INT,
          complete_address TEXT
      )
  """
  session.execute(table_creation_query)

### Select only the columns needed for Cassandra table
- Code:
  ```bash
  cassandraDF = parsedStreamDF.select("full_name", "calculated_age", "complete_address")

### Function to save DataFrame to Cassandra
- Code:
  ```bash
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

### Start the streaming query
- Code:
  ```bash
  checkpoint_location = "./checkpoint/data"
  query = save_to_cassandra(cassandraDF, keyspace, table_name, checkpoint_location)
  query.start().awaitTermination()

