package com.ggsi

import org.apache.spark.sql.SparkSession
import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import com.databricks.spark.avro._
import org.apache.avro.Schema

object AvroReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("avro file reader").master("local").getOrCreate()
    val sqlContext = spark.sqlContext
    val inputPath = "hdfs://IMPETUS-NL203:54310/"
    val fs = FileSystem.get(new URI(inputPath), spark.sparkContext.hadoopConfiguration)
    val inputPathExists = fs.exists(new Path(inputPath))
    val schema = fs.open(new Path(inputPath + "/data/schema/airports.avsc"))
    val df = sqlContext.read.format("com.databricks.spark.avro").option("header", false).option("schema", schema.toString()).load(inputPath + "/data/decomposed/airports.avro")
    df.show()
  }
}