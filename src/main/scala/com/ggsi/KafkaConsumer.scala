package com.ggsi

import java.util
import java.util.Properties

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val TOPIC = "topic1"

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "topic1")

    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(util.Collections.singletonList(TOPIC))

    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) {
        println(record)
      }
    }
    println("message consumed successfully")
  }
}