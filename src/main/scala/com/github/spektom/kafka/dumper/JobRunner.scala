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

import java.util.TimeZone

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object JobRunner {

  private def createContext(options: Options): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("kafka-dumper")
      .set("spark.cleaner.ttl", "3600")
      .set("spark.rdd.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.kryo.registrationRequired", "true")
      .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .set("spark.hadoop.mapred.output.committer.class", classOf[DirectOutputCommitter].getName)

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    new StreamingContext(spark.sparkContext, Seconds(options.interval))
  }

  def run(options: Options): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val sc = createContext(options)

    new KafkaStream(options)
      .consume(sc, new RecordSaver(options))

    sc.start()
    sc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    run(Options.parse(args))
  }
}
