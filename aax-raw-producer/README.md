# Overview

Kafka Producer to watch a directory for (.csv) files and read the data as new files are written to the input directory. Each of the records in the input file will be converted and written to a topic (based on the avro schema for Inventory data defined within application).It will convert a CSV on the fly to the strongly typed data types, i.e. avro.

# Building

`mvn clean package`

# Running

`java -jar target/aax-raw-producer-1.0-DEMO.jar`

With custom settings TOPIC_NAME, INPUT_PATH, KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL  (e.g. for custom INPUT_PATH):


`INPUT_PATH={target-folder java} -jar target/aax-raw-producer-1.0-DEMO.jar`

# Notes:

To add :

  - process, finish and error folders 
  - {Dosyadaki csv lere bakma ve okuma mevzusunu tamamla}
  - additional ?
