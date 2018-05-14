package com.github.spektom.kafka.dumper

import scopt._

/**
  * Command line arguments passed to the Spark job application
  */
case class Options(brokers: Seq[String] = Seq(),
                   topics: Seq[String] = Seq(),
                   group: String = "",
                   interval: Int = 30,
                   path: String = "")

object Options {
  def parse(args: Array[String]) = {

    new OptionParser[Options]("kafka-dumper") {

      opt[String]("brokers").action((x, c) =>
        c.copy(brokers = x.split(","))).text("Comma separated list of Kafka bootstrap servers")

      opt[String]("group").action((x, c) =>
        c.copy(group = x)).text("Kafka consumer group")

      opt[String]("topics").action((x, c) =>
        c.copy(topics = x.split(","))).text("Comma separated list of Kafka topics")

      opt[String]("interval").optional().action((x, c) =>
        c.copy(interval = x.toInt)).text("Spark batch interval in seconds (default: 30 secs)")

      opt[String]("path").action((x, c) =>
        c.copy(path = x)).text("Target destination path under which files will be saved")

      help("help").text("prints this usage text")

    }.parse(args, Options()) match {
      case Some(config) => config
      case None => throw new RuntimeException("Wrong usage")
    }
  }
}