package com.github.spektom.kafka.dumper

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.{FileAlreadyExistsException, JobConf}
import org.joda.time.format.DateTimeFormat

class FileNamingStrategy extends MultipleTextOutputFormat[Any, Any] {

  val timeFormat = DateTimeFormat.forPattern("YYYYMMddHH")

  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    val metadata = key.asInstanceOf[RecordMetadata]
    "topic=" + metadata.topic + "/dt=" + timeFormat.print(metadata.timestamp) +
      "/" + "%05d".format(metadata.partition) + "-" + "%019d".format(metadata.offset)
  }

  override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = {
    try {
      super.checkOutputSpecs(ignored, job)
    } catch {
      // We swallow this exception here to allow writing under the same path with
      // different key that will constitute the suffix
      case _: FileAlreadyExistsException =>
    }
  }
}