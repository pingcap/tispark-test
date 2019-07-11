package com.pingcap.tispark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object BatchWritePressureTest {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      throw new Exception("wrong arguments!")
    }

    val path = args(0)
    val database = args(1)
    val table = args(2)
    val skipCommitSecondaryKey = args(3)
    val lockTTLSeconds = if(args.length > 4) args(4) else "3600"
    val writeConcurrency = if(args.length > 5) args(5) else "0"

    println(
      s"""
        |path=$path
        |database=$database
        |table=$table
        |skipCommitSecondaryKey=$skipCommitSecondaryKey
        |lockTTLSeconds=$lockTTLSeconds
        |writeConcurrency=$writeConcurrency
      """.stripMargin)

    val sparkConf = new SparkConf()
      .set("spark.tispark.write.enable", "true")
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)
      .setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
      .setIfMissing("tidb.addr", "172.0.0.1")
      .setIfMissing("tidb.port", "4000")
      .setIfMissing("tidb.user", "root")
      .setIfMissing("tidb.password", "")
      .setIfMissing("spark.tispark.pd.addresses", "172.0.0.1:2379")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    val schema = StructType(Array(
      StructField("O_ORDERKEY",IntegerType),
      StructField("O_CUSTKEY",IntegerType),
      StructField("O_ORDERSTATUS",StringType),
      StructField("O_TOTALPRICE",DecimalType(15, 2)),
      StructField("O_ORDERDATE",DateType),
      StructField("O_ORDERPRIORITY",StringType),
      StructField("O_CLERK",StringType),
      StructField("O_SHIPPRIORITY",IntegerType),
      StructField("O_COMMENT",StringType)
    )
    )

    val df = spark.read.schema(schema).option("sep", "|").csv(path)
    println(s"count=${df.count()}")

    val start = System.currentTimeMillis()
    df.write
      .format("tidb")
      .option("database", database)
      .option("table", table)
      .option("skipCommitSecondaryKey", skipCommitSecondaryKey)
      .option("lockTTLSeconds", lockTTLSeconds)
      .option("writeConcurrency", writeConcurrency)
      .mode("append")
      .save()
    val end = System.currentTimeMillis()
    val seconds = (end - start) / 1000
    println(s"total time: $seconds seconds")
  }
}
