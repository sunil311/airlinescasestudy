package com.ggsi

import org.apache.spark.sql.SparkSession
import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import com.ggsi.utils.CustomSchema

object ParquetReader {

  def main(args: Array[String]): Unit = {

    val schema = CustomSchema.getParquetSchema()
      
    val spark = SparkSession.builder().appName("parquet file reader").master("local").getOrCreate()
    val sqlContext = spark.sqlContext
    val inputPath = "hdfs://IMPETUS-NL203:54310/"
    val fs = FileSystem.get(new URI(inputPath), spark.sparkContext.hadoopConfiguration)
    val inputPathExists = fs.exists(new Path(inputPath))
    val df = sqlContext.read.schema(schema).parquet(inputPath + "/data/modelled/*")
    df.show()
    //df.take(100).foreach(println)

  }
}