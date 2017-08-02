package com.ggsi.config

import com.typesafe.config.ConfigFactory

trait AppConfig {
  private val config = ConfigFactory.load()

  private val hadoop = config.getConfig("hadoop")
  private val avroData = config.getConfig("avro")
  private val parquetData = config.getConfig("parquet")
  private val kafka = config.getConfig("kafka")

  val hadoopUrl = hadoop.getString("url")

  //carriers-avro
  val carrierAvroSchema = avroData.getString("carrier.schemafile")
  val carrierAvroFileInputPath = avroData.getString("carrier.inputPath")
  val carrierAvroFileOutputPath = avroData.getString("carrier.outputPath")

  //airports-avro
  val airportsAvroSchema = avroData.getString("airports.schemafile")
  val airportsAvroFileInputPath = avroData.getString("airports.inputPath")
  val airportsAvroFileOutputPath = avroData.getString("airports.outputPath")

  //kafka-carrier
  val carrierBrokers = kafka.getString("carrier.brokers")
  val carrierTopic = kafka.getString("carrier.topic")
  val carrierClientId = kafka.getString("carrier.clientId")
  val carrierSource = kafka.getString("carrier.source")

  //kafka-airports
  val airportsBrokers = kafka.getString("airports.brokers")
  val airportsTopic = kafka.getString("airports.topic")
  val airportsClientId = kafka.getString("airports.clientId")
  val airportsSource = kafka.getString("airports.source")

  //kafka-planedata
  val planeDataBrokers = kafka.getString("planeData.brokers")
  val planeDataTopic = kafka.getString("planeData.topic")
  val planeDataClientId = kafka.getString("planeData.clientId")
  val planeDataSource = kafka.getString("planeData.source")

  //parquet
  val parquetFileInputpath = parquetData.getString("inputPath")
  val parquetFileOutputpath = parquetData.getString("outputPath")

}