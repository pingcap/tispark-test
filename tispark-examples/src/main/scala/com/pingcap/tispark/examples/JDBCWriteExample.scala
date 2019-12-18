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

import java.sql.{Connection, Driver, DriverManager}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, DriverWrapper, JDBCOptions}

/**
  * Insert into TiDB using Spark+JDBC
  *
  * please create table before run the program
  * CREATE TABLE MyData(id INT, value BIGINT, day varchar(64))
  *
  * Usage: JDBCWriteExample <tidbIP> <tidbPort> <tidbDatabase> <day>
  */
object JDBCWriteExample {

  case class MyData(id: Int, value: Long, day: String)

  val JDBC_DRIVER = "com.mysql.jdbc.Driver"

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: <tidbIP> <tidbPort> <tidbDatabase> <day>")
      System.exit(1)
    }

    val tidbIP = args(0)
    val tidbPort = args(1)
    val tidbDatabase = args(2)
    val day = args(3)

    val tidbUser = "root"
    val tidbPassword = ""
    val tidbTable = "MyData"

    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)
      .setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
      .setIfMissing("spark.tispark.pd.addresses", "127.0.0.1:2379")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    import spark.implicits._

    val jdbcUrl = s"jdbc:mysql://address=(protocol=tcp)(host=$tidbIP)(port=$tidbPort)/$tidbDatabase?" +
      s"user=$tidbUser&password=$tidbPassword&useUnicode=true&characterEncoding=UTF-8&useSSL=false" +
      s"&rewriteBatchedStatements=true&autoReconnect=true&maxReconnects=10&allowMultiQueries=true"

    // 1. delete dirty data
    val tidbConnection = createConnectionFactory(jdbcUrl)()
    val statement = tidbConnection.createStatement()
    statement.executeUpdate(s"DELETE FROM `$tidbTable` WHERE day='$day'")
    statement.close()
    tidbConnection.close()

    // 2. generate data
    val df = spark.sparkContext.makeRDD(1 to 1000).map { id =>
      MyData(id, System.currentTimeMillis(), day)
    }.toDF()

    // 3. insert data into TiDB
    df.write
      .mode(saveMode = "append")
      .format("jdbc")
      .option("driver", JDBC_DRIVER)
      // replace the host and port with yours and be sure to use rewrite batch
      .option("url", jdbcUrl)
      .option("useSSL", "false")
      // as tested, setting to `150` is a good practice
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 150)
      .option("dbtable", tidbTable) // database name and table name here
      .option("isolationLevel", "NONE") // set isolationLevel to NONE
      .option("user", "root") // TiDB user here
      .save()
  }

  private def createConnectionFactory(jdbcURL: String): () => Connection = {
    import scala.collection.JavaConverters._
    val driverClass: String = JDBC_DRIVER
    () => {
      DriverRegistry.register(driverClass)
      val driver: Driver = DriverManager.getDrivers.asScala
        .collectFirst {
          case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == driverClass => d
          case d if d.getClass.getCanonicalName == driverClass => d
        }
        .getOrElse {
          throw new IllegalStateException(
            s"Did not find registered driver with class $driverClass"
          )
        }
      driver.connect(jdbcURL, new Properties())
    }
  }
}
