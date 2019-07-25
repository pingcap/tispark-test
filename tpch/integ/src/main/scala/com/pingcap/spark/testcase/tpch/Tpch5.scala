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


class Tpch5(spark: SparkSession, prop: Properties) extends TestBase(spark, prop) {
  override def tidbQuery =
    """
      select
 |           n_name,
 |           sum(l_extendedprice * (1 - l_discount)) as revenue
 |        from
 |           customer,
 |           orders,
 |           lineitem,
 |           supplier,
 |           nation,
 |           region
 |        where
 |           c_custkey = o_custkey
 |           and l_orderkey = o_orderkey
 |           and l_suppkey = s_suppkey
 |           and c_nationkey = s_nationkey
 |           and s_nationkey = n_nationkey
 |           and n_regionkey = r_regionkey
 |           and r_name = 'ASIA'
 |           and o_orderdate >= '1994-01-01'
 |           and o_orderdate < '1995-01-01'
 |        group by
 |           n_name
 |        order by
 |           revenue desc
    """.stripMargin
}
