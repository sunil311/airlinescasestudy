package com.ggsi.utils

import java.util.Properties
import com.ggsi.config.AppConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.consumer.KafkaConsumer

class KafkaUtils extends AppConfig {
  def initialize(clientId: String, brokers: String) = {
    val brokers: String = "localhost:9092,localhost:9092"
    //val TOPIC = "Planedate"
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", clientId)
    props.put("acks", "0")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  def getProducer(clientId: String, brokers: String) = {
    val producer = new KafkaProducer[Nothing, String](initialize(clientId, brokers))
    producer
  }

  def getConsumer(clientId: String, brokers: String) = {
    val consumer = new KafkaConsumer[Nothing, String](initialize(clientId, brokers))
    consumer
  }
}

object KafkaUtils {
  private val kafkaUtils = new KafkaUtils

  def getProducer(clientId: String, brokers: String): KafkaProducer[Nothing, String] = {

    kafkaUtils.getProducer(clientId, brokers)
  }

  def getConsumer(clientId: String, brokers: String): KafkaConsumer[Nothing, String] = {

    kafkaUtils.getConsumer(clientId, brokers)
  }
}