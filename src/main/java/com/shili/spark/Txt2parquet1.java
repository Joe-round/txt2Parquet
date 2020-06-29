package com.shili.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;


public class Txt2parquet1 {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("local[*]")
                .master("local[*]")
                .getOrCreate();
        JavaRDD<String> source = spark.read().textFile("/Users/mac/documentation/input.txt").javaRDD();

        JavaRDD<Row> rowJavaRDD = source.map(new Function<String, Row>() {
            public Row call(String v1) throws Exception {
                String[] lines = v1.split(",");
                String name = lines[0].trim();
                String age = lines[1].trim();
                String n = lines[2].trim();
                String a = lines[3].trim();
                String e = lines[4].trim();
                String ae = lines[5].trim();

                return RowFactory.create(name, age, n, a, e, ae);
            }
        });

        ArrayList<StructField> fields = new ArrayList<StructField>();
        StructField field = null;
        field = DataTypes.createStructField("name", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("age", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("n", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("a", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("e", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("ae", DataTypes.StringType, true);
        fields.add(field);

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> dataFrame = spark.createDataFrame(rowJavaRDD, schema);

        dataFrame.write().mode(SaveMode.Append).parquet("/Users/mac/documentation/parquet");

        Dataset<Row> parquet = spark.read().parquet("/Users/mac/documentation/parquet");

        parquet.printSchema();

        parquet.select("name","age","n","a","e","ae").show();

        spark.stop();


    }
}
