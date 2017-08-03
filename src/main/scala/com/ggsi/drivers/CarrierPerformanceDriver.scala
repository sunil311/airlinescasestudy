package com.ggsi.drivers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import com.ggsi.config.AppConfig

import org.apache.spark.sql.functions._
import com.ggsi.utils.CustomSchema


object CarrierPerformanceDriver extends AppConfig{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("carrier performace").master("local").getOrCreate()
    val sqlContext = spark.sqlContext
    
    val schema = CustomSchema.getParquetSchema()
      
    val df = sqlContext.read.schema(schema).parquet(hadoopUrl + parquetFileOutputpath+"*")
    val aggCarriers = df.select("UniqueCarrier","CarrierDelay").groupBy("UniqueCarrier").sum("CarrierDelay")
    aggCarriers.show()
  }
}