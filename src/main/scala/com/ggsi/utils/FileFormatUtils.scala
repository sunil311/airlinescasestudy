package com.ggsi.utils

import org.apache.spark.sql.SparkSession
import java.net.URI
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import com.ggsi.config.AppConfig
import com.databricks.spark.avro._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import jdk.nashorn.internal.codegen.types.LongType
import org.apache.spark.sql.types.LongType

object FileFormatUtils extends AppConfig {

  def convertCSVtoAvro(spark: SparkSession, inputPath: String, schemaPath: String, outputPath: String, outputFileName: String): Unit = {
    val sqlContext = spark.sqlContext

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "false")
      .option("schema", getSchema(spark, schemaPath).toString())
      .load(hadoopUrl + inputPath)
    df.write.avro(hadoopUrl + carrierAvroFileOutputPath + outputFileName);
  }

  def getSchema(spark: org.apache.spark.sql.SparkSession, schemaPath: String) = {
    val fs = FileSystem.get(new URI(hadoopUrl), spark.sparkContext.hadoopConfiguration)
    val inputPathExists = fs.exists(new Path(hadoopUrl))

    if (!inputPathExists) {
      println("Input Path does not exists")
      //return
    }
    val schema = fs.open(new Path(hadoopUrl + schemaPath))
    schema
  }

  def convertCSVtoParquet(spark: SparkSession, inputPath: String, schemaPath: String, outputPath: String, outputFileName: String): Unit = {

    val schema = CustomSchema.getParquetSchema()

    val sqlContext = spark.sqlContext

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .schema(schema)
      .option("delimiter", ",")
      .option("header", "true")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("mode", "DROPMALFORMED")
      .load(hadoopUrl + inputPath)

    //df.show
    df.write.parquet(hadoopUrl + outputPath + outputFileName)
  }

}