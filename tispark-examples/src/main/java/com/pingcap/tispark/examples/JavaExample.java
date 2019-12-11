package com.pingcap.tispark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class JavaExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("TiSparkJavaExample")
        .set("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
        .set("spark.tispark.pd.addresses", "127.0.0.1:2379");
    SparkSession spark = SparkSession
        .builder()
        .config(conf)
        .getOrCreate();

    spark.sql("show databases").show();
    spark.sql("use tpch_test");
    spark.sql("select count(*) from lineitem").show();
  }
}
