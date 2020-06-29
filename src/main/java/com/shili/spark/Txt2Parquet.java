package com.shili.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Txt2Parquet {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("local[*]")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> text = spark.read().text("/Users/mac/documentation/input.txt");

        text.write().mode(SaveMode.Append).parquet("/Users/mac/documentation/parquet");

//        JavaRDD<Row> rowJavaRDD = spark.read().parquet("/Users/mac/documentation/parquet").toJavaRDD();
//        rowJavaRDD.foreach(new VoidFunction<Row>() {
//            public void call(Row row) throws Exception {
//                System.out.println(row);
//            }
//        });
        Dataset<Row> parquet = spark.read().parquet("/Users/mac/documentation/parquet");

        parquet.printSchema();

        parquet.select("*").show();


        spark.stop();


    }
}
