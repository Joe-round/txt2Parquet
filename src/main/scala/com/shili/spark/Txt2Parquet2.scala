package com.shili.spark

import org.apache.spark.sql.SparkSession

object Txt2Parquet2 {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("Txt2Parquet").master("local[*]").getOrCreate();

    val text = spark.read.textFile("/Users/mac/documentation/input.txt")

    text.write.parquet("/Users/mac/documentation/parquet")


    val parquet = spark.read.parquet("/Users/mac/documentation/parquet")

    parquet.select("*").show()
    spark.stop();

  }
}
