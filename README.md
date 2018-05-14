kafka-dumper
=============

Dumps events stored in Kafka to any Hadoop supported file system using Spark Streaming.

The process reads events in slices (slice size is defined by the `interval` argument), then
writes each slice to a text file using the following format:

    topic=<Kafka topic>/dt=YYYYMMddHH/<Kafka Partition>-<Kafka Last Offset>.gz
    
Kafka internal timestamp is used for determining what bucket an event will be 
written to.
  
To tweak the format, see `com.github.spektom.kafka.dumper.FileNamingStrategy` class.

## Building

    mvn package
    
## Running

USAGE:

    spark-submit kafka-dumper_2.11-0.0.1-uberjar.jar [OPTIONS]

Where options are:

    --brokers <value>   Comma separated list of Kafka bootstrap servers
    --topics <value>    Comma separated list of Kafka topics
    --group <value>     Kafka consumer group
    --interval <value>  Spark batch interval in seconds (default: 30 secs)
    --path <value>      Target destination path under which files will be saved

For example, the following command will read events from local Kafka topic called `events`, 
and write them to local files under `/tmp/datalake` directory every 10 seconds:

    ./spark-submit kafka-dumper_2.11-0.0.1-uberjar.jar \
      --brokers localhost:9092 --topics events
      --path /tmp/datalake --group kafka-dumper
      --interval 10
