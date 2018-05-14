package com.github.spektom.kafka.dumper

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD

trait RecordProcessor[K, V] {

  def process(topicPartitionRanges: Map[String, Map[Int, (Long, Long)]],
              rdd: RDD[ConsumerRecord[K, V]]): Unit
}
