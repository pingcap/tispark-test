/*
 * Copyright 2019 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * before run the code in IDE, please enable maven profile `local-debug`
 */
object TiDataSourceExampleWithExtensions {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setIfMissing("spark.tispark.write.allow_spark_sql", "true")
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)
      .setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
      .setIfMissing("spark.tispark.tidb.addr", "127.0.0.1")
      .setIfMissing("spark.tispark.tidb.port", "4000")
      .setIfMissing("spark.tispark.pd.addresses", "127.0.0.1:2379")
      //.setIfMissing("spark.tispark.write.without_lock_table", "true")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val sqlContext = spark.sqlContext

    readUsingScala(sqlContext)

    //writeUsingScala(sqlContext)

    //useAnotherTiDB(sqlContext)
  }

  def readUsingScala(sqlContext: SQLContext): Unit = {
    // use tidb config in spark config if does not provide in data source config
    val tidbOptions: Map[String, String] = Map(
      "tidb.user" -> "root",
      "tidb.password" -> ""
    )
    val df = sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", "tpch_test")
      .option("table", "CUSTOMER")
      .load()
      .filter("C_CUSTKEY = 1")
      .select("C_NAME")
    df.show()
  }

  def writeUsingScala(sqlContext: SQLContext): Unit = {
    /* create table before run the code
    CREATE TABLE tpch_test.target_table_orders (
      `O_ORDERKEY` int(11) NOT NULL,
      `O_CUSTKEY` int(11) NOT NULL,
      `O_ORDERSTATUS` char(1) NOT NULL,
      `O_TOTALPRICE` decimal(15,2) NOT NULL,
      `O_ORDERDATE` date NOT NULL,
      `O_ORDERPRIORITY` char(15) NOT NULL,
      `O_CLERK` char(15) NOT NULL,
      `O_SHIPPRIORITY` int(11) NOT NULL,
      `O_COMMENT` varchar(79) NOT NULL
    )
     */

    // use tidb config in spark config if does not provide in data source config
    val tidbOptions: Map[String, String] = Map("tidb.user" -> "root", "tidb.password" -> "")

    // data to write
    val df = sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", "tpch_test")
      .option("table", "ORDERS")
      .load()

    // Append
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", "tpch_test")
      .option("table", "target_table_orders")
      .mode("append")
      .save()
  }

  def useAnotherTiDB(sqlContext: SQLContext): Unit ={
    // tidb config priority: data source config > spark config
    val tidbOptions: Map[String, String] = Map(
      "tidb.addr" -> "anotherTidbIP",
      "tidb.password" -> "",
      "tidb.port" -> "4000",
      "tidb.user" -> "root",
      "spark.tispark.pd.addresses" -> "pd0:2379"
    )

    val df = sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", "tpch_test")
      .option("table", "CUSTOMER")
      .load()
      .filter("C_CUSTKEY = 1")
      .select("C_NAME")
    df.show()
  }
}
