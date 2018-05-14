package com.github.spektom.kafka.dumper

import com.esotericsoftware.kryo.Kryo

class KryoRegistrator extends org.apache.spark.serializer.KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"))
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"))
  }
}