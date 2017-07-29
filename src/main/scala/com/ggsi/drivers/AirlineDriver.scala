package com.ggsi.drivers

import com.ggsi.config.AppConfig
import com.ggsi.utils.FileFormatUtils
import com.ggsi.spark.SparkContextFactory

object AirlineDriver extends AppConfig {

  def main(args: Array[String]): Unit = {
    //FileFormatUtils.convertCSVtoAvro(SparkContextFactory.getSparkSession("local"), carrierAvroFileInputPath, carrierAvroSchema, carrierAvroFileOutputPath, "carrier.avro")
    FileFormatUtils.convertCSVtoAvro(SparkContextFactory.getSparkSession("local"), airportsAvroFileInputPath, airportsAvroSchema, airportsAvroFileOutputPath, "airports.avro")
    
  }
}