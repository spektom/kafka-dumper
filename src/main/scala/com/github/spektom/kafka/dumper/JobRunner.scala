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
