package com.ggsi

import java.util.Properties

import org.apache.kafka.clients.producer._

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val TOPIC = "topic1"

    for (i <- 1 to 5) {
      val record = new ProducerRecord(TOPIC, "key", s"hello $i")
      try{
      producer.send(record)
      }catch {
         case ex: Exception =>{
            println(ex.printStackTrace())
         }
      }
    }

    val record = new ProducerRecord(TOPIC, "key", "the end " + new java.util.Date)
    producer.send(record)

    producer.close()
    println("message published successfully")
  }
}