package com.ggsi.drivers

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import com.ggsi.spark.SparkContextFactory

object Test {
  def main(args: Array[String]): Unit = {

    val schema = StructType(Array(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("sex", StringType, true),
      StructField("salary", LongType, true)))

    val spark = SparkContextFactory.getSparkSession("local")
    val sqlContext = spark.sqlContext

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .schema(schema)
      .option("delimiter", ",")
      .option("nullValue", " ")
      .option("treatEmptyValuesAsNulls", "true")
      //.option("ignoreLeadingWhiteSpace","true")
      //.option("ignoreTrailingWhiteSpace","true")
      .option("mode", "DROPMALFORMED")
      .load("/home/impadmin/Desktop/test.csv")
    df.show()
  }

}