package com.rachel.templates.utils

import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

object KafkaPro {
  val kafkaProducerConfig = {
    val p = new Properties()
    p.setProperty("bootstrap.servers", "kafka01:9092,kafka02:9092,flume01:9092")
    p.setProperty("key.serializer", classOf[StringSerializer].getName)
    p.setProperty("value.serializer", classOf[StringSerializer].getName)
    p.setProperty("auto.commit.interval.ms", "1000")
    p.setProperty("zookeeper.sync.time.ms", "200")
    p.setProperty("zookeeper.session.timeout.ms", "400")
    p.setProperty("zookeeper.connect", "133.71.24.221:2181,133.71.24.222:2181,133.71.24.223:2181/kafka")
    p
  }
  def getKafkaProducer(ssc: StreamingContext): Broadcast[KafkaSink[String, String]] = {
    val kafkaProducer = ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    kafkaProducer
  }
}