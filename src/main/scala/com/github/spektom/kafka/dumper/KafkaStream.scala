/*
 * Copyright (c) 2017-present Michael Spector
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
