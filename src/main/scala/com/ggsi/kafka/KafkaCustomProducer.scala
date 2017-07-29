package com.ggsi.kafka

import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import scala.io.Source
import com.ggsi.utils.KafkaUtils
import com.ggsi.config.AppConfig

object KafkaCustomProducer extends AppConfig {
  def main(args: Array[String]): Unit = {
    kafkaCustomProducer(carrierClientId, carrierBrokers, carrierSource, carrierTopic)
  }

  def kafkaCustomProducer(clientId: String, brokers: String, source: String, topic: String) = {
    val producer = KafkaUtils.getProducer(clientId, brokers)
    println("Started.....")
    val bufferedSource = Source.fromFile(source)
    for (line <- bufferedSource.getLines) {
      val record = new ProducerRecord(topic, line)
      producer.send(record)
    }
    bufferedSource.close
    producer.close()
    println("message published successfully")
  }
}