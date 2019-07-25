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

package com.pingcap.spark.testcase.tpch

import java.util.Properties

import com.pingcap.spark.TestBase
import org.apache.spark.sql.SparkSession


class Tpch3(spark: SparkSession, prop: Properties) extends TestBase(spark, prop) {
  override def tidbQuery =
    """
      select
 |           l_orderkey,
 |           sum(l_extendedprice * (1 - l_discount)) as revenue,
 |           o_orderdate,
 |           o_shippriority
 |        from
 |           customer,
 |           orders,
 |           lineitem
 |        where
 |           c_mktsegment = 'BUILDING'
 |           and c_custkey = o_custkey
 |           and l_orderkey = o_orderkey
 |           and o_orderdate < '1995-03-15'
 |           and l_shipdate > '1995-03-15'
 |        group by
 |           l_orderkey,
 |           o_orderdate,
 |           o_shippriority
 |        order by
 |           revenue desc,
 |           o_orderdate
    """.stripMargin
}
