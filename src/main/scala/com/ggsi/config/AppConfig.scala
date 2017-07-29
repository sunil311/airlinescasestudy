package com.ggsi.config

import com.typesafe.config.ConfigFactory

trait AppConfig {
  private val config = ConfigFactory.load()

  private val hadoop = config.getConfig("hadoop")
  private val avroData = config.getConfig("avro")

  //carriers-avro
  val hadoopUrl = hadoop.getString("url")
  val carrierAvroSchema = avroData.getString("carrier.schemafile")
  val carrierAvroFileInputPath = avroData.getString("carrier.inputPath")
  val carrierAvroFileOutputPath = avroData.getString("carrier.outputPath")

  //airports-avro
  val airportsAvroSchema = avroData.getString("airports.schemafile")
  val airportsAvroFileInputPath = avroData.getString("airports.inputPath")
  val airportsAvroFileOutputPath = avroData.getString("airports.outputPath")

  //kafka-carrier
  val carrierBrokers = avroData.getString("carrier.brokers")
  val carrierTopic = avroData.getString("carrier.topic")
  val carrierClientId = avroData.getString("carrier.clientId")
  val carrierSource = avroData.getString("carrier.source")

  //kafka-airports
  val airportsBrokers = avroData.getString("airports.brokers")
  val airportsTopic = avroData.getString("airports.topic")
  val airportsClientId = avroData.getString("airports.clientId")
  val airportsSource = avroData.getString("airports.source")

  //kafka-planedata
  val planeDataBrokers = avroData.getString("planeData.brokers")
  val planeDataTopic = avroData.getString("planeData.topic")
  val planeDataClientId = avroData.getString("planeData.clientId")
  val planeDataSource = avroData.getString("planeData.source")

}