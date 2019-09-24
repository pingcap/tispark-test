package com.pingcap.tispark.examples

import com.pingcap.tispark.TiDBUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, TiContext}

object Test {
  def main(args: Array[String]): Unit = {
    var x: Int = 100
    var y: Int = 1000
    if (!args.isEmpty && args.length != 2) {
      throw new RuntimeException("Invalid arg input, should be in form of \"x y\"")
    } else if (args.length == 2) {
      x = args(0).toInt
      y = args(1).toInt
      if (x <= 0 || y <= 0) {
        throw new RuntimeException("Invalid arg input, x y should be greater than 0")
      }
    }
    println(s"x=$x y=$y")
    // init
    val start = System.currentTimeMillis()
    // spark.driver.extraJavaOptions="-Dio.netty.maxDirectMemory=0 -XX:MaxDirectMemorySize=10M"
    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)
      .setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
      .setIfMissing("tidb.addr", "localhost")
      .setIfMissing("tidb.port", "4000")
      .setIfMissing("tidb.user", "root")
      .setIfMissing("spark.tispark.pd.addresses", "localhost:2379")

    val spark = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()

    // jdbc
    val jdbcUrl =
      s"jdbc:mysql://address=(protocol=tcp)(host=127.0.0.1)(port=4000)/?user=root&password=" +
        s"&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false" +
        s"&rewriteBatchedStatements=true&autoReconnect=true&failOverReadOnly=false&maxReconnects=10" +
        s"&allowMultiQueries=true"
    val _tidbConnection = TiDBUtils.createConnectionFactory(jdbcUrl)()

    // show
    spark.sql("show databases").show()

    // load tidb
    _tidbConnection.setCatalog("test")
    val tidb = _tidbConnection.createStatement()
    tidb.execute("drop table if exists t")
    tidb.execute("create table t(c1 int, c2 int)")
    val s = StringBuilder.newBuilder
    s.append("(-1, -1)")
    for (i <- 0 until x) {
      for (j <- i until y) {
        s.append(s",($i, $j)")
      }
    }
    for (_ <- 0 until 100) {
      tidb.execute(s"insert into t values$s")
    }

    // load spark
    spark.sql("use default")
    spark.sql("drop table if exists t")
    spark.sql("create table t(c1 int, c2 int)")

//    spark.sql(s"insert overwrite table t select x.c1, x.c2 from test.t x join test.t y on x.c1 = y.c1").explain
//    for (_ <- 0 until 100) {
      spark.sql(s"insert overwrite table t select * from test.t").explain
//    }
    spark.sql("select count(*) from t").show
    spark.sql("select * from t").show
  }
}
