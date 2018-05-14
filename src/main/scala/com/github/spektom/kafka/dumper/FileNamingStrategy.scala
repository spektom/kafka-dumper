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
