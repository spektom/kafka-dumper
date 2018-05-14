kafka-dumper
=============

Dumps events stored in Kafka to any Hadoop supported file system using Spark Streaming.

The process reads events in slices (slice size is defined by the `interval` argument), then
writes each slice to a text file using predefined output file format.

## Output format

The output format is defined as follows:

    topic=<Kafka topic>/dt=YYYYMMddHH/<Kafka Partition>-<Kafka Starting Offset>.gz
    
Kafka internal timestamp is used for determining what bucket an event will be 
written to.
  
To tweak the format, see `com.github.spektom.kafka.dumper.FileNamingStrategy` class.

## Delivery semantics

Consumed offsets are stored in Kafka itself right after events were written to files. If the process
crashes in between, the next time it starts it will start consuming events from the previously
commited offsets, and the target files will be overwritten. Idempotency during writes guarantees
exactly once semantics in this case.

## Supported event formats

Any text-based event format is supported: JSON, CSV, etc.

For binary format support, some tweaks must be provided for `com.github.spektom.kafka.dumper.KafkaStream`
and `com.github.spektom.kafka.dumper.RecordSaver` classes

## Building and running

Building shadow Jar file:

    mvn package
    
Usage:

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

