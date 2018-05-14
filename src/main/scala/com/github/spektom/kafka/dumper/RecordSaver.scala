package com.github.spektom.kafka.dumper

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.reflect._

class RecordSaver[K: ClassTag, V: ClassTag](options: Options) extends RecordProcessor[K, V] {

  override def process(topicPartitionRanges: Map[String, Map[Int, (Long, Long)]],
                       rdd: RDD[ConsumerRecord[K, V]]): Unit = {
    rdd.map { record =>
      val metadata = new RecordMetadata(
        topicPartitionRanges(record.topic())(record.partition())._2,
        record.partition(),
        record.topic(),
        record.timestamp()
      )
      (metadata, record.value)
    }
      .saveAsHadoopFile(options.path, classTag[K].runtimeClass, classTag[V].runtimeClass,
        classOf[FileNamingStrategy], classOf[GzipCodec])
  }
}
