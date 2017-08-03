package com.ggsi.utils

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

class CustomSchema {

  def getParquetSchema(): StructType = {

    val schema = StructType(Array(
      StructField("Year", IntegerType, true),
      StructField("Month", IntegerType, true),
      StructField("DayofMonth", IntegerType, true),
      StructField("DayOfWeek", IntegerType, true),
      StructField("DepTime", LongType, true),
      StructField("CRSDepTime", LongType, true),
      StructField("ArrTime", LongType, true),
      StructField("CRSArrTime", LongType, true),
      StructField("UniqueCarrier", StringType, true),
      StructField("FlightNum", StringType, true),
      StructField("TailNum", StringType, true),
      StructField("ActualElapsedTime", LongType, true),
      StructField("CRSElapsedTime", LongType, true),
      StructField("AirTime", LongType, true),
      StructField("ArrDelay", LongType, true),
      StructField("DepDelay", LongType, true),
      StructField("Origin", StringType, true),
      StructField("Dest", StringType, true),
      StructField("Distance", StringType, true),
      StructField("TaxiIn", LongType, true),
      StructField("TaxiOut", LongType, true),
      StructField("Cancelled", StringType, true),
      StructField("CancellationCode", StringType, true),
      StructField("Diverted", StringType, true),
      StructField("CarrierDelay", LongType, true),
      StructField("WeatherDelay", LongType, true),
      StructField("NASDelay", LongType, true),
      StructField("SecurityDelay", LongType, true),
      StructField("LateAircraftDelay", LongType, true)))
    schema
  }
}

object CustomSchema {
  val customSchema = new CustomSchema
  def getParquetSchema(): StructType = {
    customSchema.getParquetSchema()
  }
}