package com.github.spektom.kafka.dumper

case class RecordMetadata(offset: Long, partition: Long, topic: String, timestamp: Long)
