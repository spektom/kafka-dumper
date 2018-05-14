package com.github.spektom.kafka.dumper

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

class KafkaStream(options: Options) {

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> options.brokers.mkString(","),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def consume(sc: StreamingContext, processor: RecordProcessor[String, String]): Unit = {
    val stream = KafkaUtils.createDirectStream[String, String](
      sc,
      PreferConsistent,
      Subscribe[String, String](options.topics, kafkaParams)
    )
    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val topicPartitionRanges = offsetRanges.groupBy(_.topic)
        .map { case (k, v) =>
          (k, v.map(r => (r.partition, (r.fromOffset, r.untilOffset))).toMap) }

      processor.process(topicPartitionRanges, rdd)

      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
  }
}