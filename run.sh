#!/usr/bin/env bash

### SETUP

# Download Confluent Platform https://www.confluent.io/download/
# Unzip and add confluent-{version}/bin to your PATH

# Download and install Docker for Mac / Windows / Linux and run the kafka-cluster
docker-compose up kafka-cluster
# Alternatively setup kafka cluster in your environment and manually run up the services


###need to get in the

# Create all the topics we're going to use for this demo
kafka-topics --create --topic aax-raw --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
kafka-topics --create --topic aax-core --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
kafka-topics --create --topic aax-evaluation --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
#kafka-topics --create --topic xxx --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
#kafka-topics --create --topic xxx --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181

# Build and package the different project components (make sure you have maven installed)
mvn clean package

### PLAYING

## Step 1: Inventory Raw Data Producer

# Start an avro consumer on our inventory-raw topic , adjust the command if necessary
kafka-avro-console-consumer --topic inventory-raw --bootstrap-server 127.0.0.1:9092

# And launch our first producer in another terminal !
#export INPUT_PATH="C:\Users\OzturkD\Desktop\aaa\" # Kafka for Beginners Course
java -jar inventory-raw-producer/target/inventory-raw-producer-1.0-DEMO.jar
java -jar inventory-raw-producer/target/inventory-raw-producer-1.0-DEMO-jar-with-dependencies.jar
# This pulls over the data from csv files located in the input path. It has some intention delay of 50 ms between each send so that we can see it stream in the consumer consumer


## Step 2: Kafka Streams - Inventory Aggregator

# New terminal: Start an avro consumer on our inventory-core topic
kafka-avro-console-consumer --topic inventory-core --bootstrap-server 107.0.0.1:9092

# New terminal: Start an avro consumer on our inventory-evaluation topic
kafka-avro-console-consumer --topic inventory-evaluation --bootstrap-server 127.0.0.1:9092

# Launch our inventory aggregator app
java -jar inventory-aggregator/target/inventory-aggregator-1.0-DEMO.jar
java -jar inventory-aggregator/target/inventory-aggregator-1.0-DEMO-jar-with-dependencies.jar

# Load the evaluation layer data into Solr using Kafka Connect Sink!
confluent load SinkTopics -d kafka-connectors/SinkTopicsInPostgres.properties

## Step 6: Clean up
docker-compose down