/*
 *
 * Copyright 2017 PingCAP, Inc.
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
 *
 */

package com.pingcap.spark

import java.util.Properties

import com.pingcap.spark.testcase.tpch._
import org.apache.spark.sql.{SparkSession, TiContext}

object TestFramework {
  val ConfName = "config.properties"


  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("TiSpark Integration Test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val prop: Properties = loadConf(ConfName)

    val ti = new TiContext(spark, List(prop.getProperty("pdaddr")))
    ti.tidbMapDatabase("tpch")

<<<<<<< HEAD:integ/src/main/scala/com/pingcap/spark/IntegTest.scala
    val tests: Array[TestBase] = Array(
      new Tpch1(spark, prop),
      new Tpch2(spark, prop),
=======
    val tests: Array[Tpch] = Array(
      new Tpch1(spark, prop),
//      new Tpch2(spark, prop),
>>>>>>> e4f404704bf0e5f4787f73f49f3a013cd056ac7d:integ/src/main/scala/com/pingcap/spark/TestFramework.scala
      new Tpch3(spark, prop),
      new Tpch4(spark, prop),
      new Tpch5(spark, prop),
      new Tpch6(spark, prop),
      new Tpch7(spark, prop),
      new Tpch8(spark, prop),
      new Tpch9(spark, prop),
      new Tpch10(spark, prop),
<<<<<<< HEAD:integ/src/main/scala/com/pingcap/spark/IntegTest.scala
      new Tpch11(spark, prop),
      new Tpch12(spark, prop),
      new Tpch13(spark, prop),
      new Tpch14(spark, prop),
      new Tpch16(spark, prop),
      new Tpch17(spark, prop),
      new Tpch18(spark, prop),
=======
>>>>>>> e4f404704bf0e5f4787f73f49f3a013cd056ac7d:integ/src/main/scala/com/pingcap/spark/TestFramework.scala
      new Tpch11(spark, prop),
      new Tpch12(spark, prop),
      new Tpch13(spark, prop),
      new Tpch14(spark, prop),
      new Tpch16(spark, prop),
      new Tpch17(spark, prop),
//      new Tpch19(spark, prop),
      new Tpch20(spark, prop),
//      new Tpch21(spark, prop),
      new Tpch22(spark, prop)
    )

    tests.filter(test => needRun(test.testName(), prop)).foreach {
      test => {
        println("Test for " + test.testName())
        test.test()
        println("\n")
      }
    }
    spark.close()
    System.exit(0)
  }

  def needRun(caseName: String, prop: Properties): Boolean = {
    val cases = prop.getProperty("testcases")
    if (cases.equalsIgnoreCase("all")) {
      true
    } else {
      val caseNames = cases.split(",")
      if (caseNames.exists(_.equalsIgnoreCase(caseName))) {
        true
      } else {
        caseNames.exists(_.matches(caseName))
      }
    }
  }

  def loadConf(conf: String): Properties = {
    val confStream = getClass().getClassLoader().getResourceAsStream(conf)
    val prop = new Properties();
    prop.load(confStream)
    prop
  }
}
