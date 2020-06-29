package com.shili.spark


import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


object Txt2Parquet3 {


  def main(args: Array[String]): Unit = {


      val spark = SparkSession.builder().appName("Txt2Parquet3").master("local[*]").getOrCreate()

    val txt = spark.read.textFile("/Users/mac/documentation/input.txt").rdd


    val rdd = txt.map(x => {
      val lines = x.split(",")
      Row(lines(0), lines(1), lines(2), lines(3), lines(4), lines(5))
    })

    val schema = new StructType()
      .add("name",StringType,true)
      .add("age",StringType,true)
      .add("n",StringType,true)
      .add("a",StringType,true)
      .add("e",StringType,true)
      .add("ae",StringType,true)


    val dataFrame = spark.createDataFrame(rdd,schema)

    dataFrame.write.mode(SaveMode.Append).parquet("/Users/mac/documentation/parquet")

    spark.read.parquet("/Users/mac/documentation/parquet")
      .select("name","age","n","a","e","ae")
      .show();

    spark.stop()










  }

}
