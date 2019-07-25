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


class Tpch10(spark: SparkSession, prop: Properties) extends TestBase(spark, prop) {

  override def tidbQuery =
    """
      select
 |        	c_custkey,
 |        	c_name,
 |        	sum(l_extendedprice * (1 - l_discount)) as revenue,
 |        	c_acctbal,
 |        	n_name,
 |        	c_address,
 |        	c_phone,
 |        	c_comment
 |        from
 |        	customer,
 |        	orders,
 |        	lineitem,
 |        	nation
 |        where
 |        	c_custkey = o_custkey
 |        	and l_orderkey = o_orderkey
 |        	and o_orderdate >= '1993-10-01'
 |        	and o_orderdate < '1994-01-01'
 |        	and l_returnflag = 'R'
 |        	and c_nationkey = n_nationkey
 |        group by
 |        	c_custkey,
 |        	c_name,
 |        	c_acctbal,
 |        	c_phone,
 |        	n_name,
 |        	c_address,
 |        	c_comment
 |        order by
 |        	revenue desc, c_custkey
    """.stripMargin
}
