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
        topicPartitionRanges(record.topic())(record.partition())._1, // starting offset
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
